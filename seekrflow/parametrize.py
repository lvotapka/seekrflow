"""
parametrize.py

Given a starting PDB complex with a ligand and a receptor, this script
will parametrize the components of the system for input to a 
seekr calculation.
"""

import os
import typing
import pathlib
import argparse
#import warnings
from shutil import copyfile

import numpy as np
from rdkit import Chem
from rdkit.Chem import rdForceFieldHelpers
from rdkit.Geometry import Point3D
from rdkit.Chem import Draw
import parmed
import mdtraj
import openmm
import openmm.unit as unit
import openmm.app as openmm_app
from openff.toolkit.topology import Molecule
from openff.units.openmm import to_openmm
from openmmforcefields.generators import SystemGenerator
import seekrtools.scripts.pqr_resid_for_each_atom as pqr_resid_for_each_atom

import seekrflow.modules.base as base
import seekrflow.modules.structures as structures

LIGAND_PDB_FILENAME = "ligand.pdb"
RECEPTOR_PDB_FILENAME = "receptor.pdb"
DEFAULT_LIGAND_SDF_FILENAME = "ligand.sdf"
PME_TOL = 2.5e-4
MAX_MINIMIZATION_ITERATIONS = 0
PARAM_SEEKRFLOW_GLOB = "seekrflow_pre_param_*.json"
PARAM_SEEKRFLOW_BASE = "seekrflow_pre_param_{}.json"

# Lots of warnings are thrown from Espaloma and OpenFF, so we suppress them.
#warnings.filterwarnings("ignore")

def _split_receptor_ligand(
        seekrflow: structures.Seekrflow,
        ) -> None:
    """
    Split the receptor-ligand complex into separate PDB files for the receptor and the ligand.
    """
    full_structure = parmed.load_file(seekrflow.receptor_ligand_pdb, skip_bonds=True)
    #param_dir = seekrflow.get_parametrize_directory()
    receptor_filename = os.path.join(structures.PARAMETRIZE, RECEPTOR_PDB_FILENAME)
    ligand_filename = os.path.join(structures.PARAMETRIZE, LIGAND_PDB_FILENAME)
    assert len(seekrflow.ligand_indices) > 0, \
        "No ligand indices in seekrflow object."
    ligand_serial_list:  typing.List[int] = []
    for ligand_index in seekrflow.ligand_indices:
        ligand_serial_list.append(full_structure.atoms[ligand_index].number)
    ligand_selection_str = f"@{','.join(map(str, ligand_serial_list))}"
    receptor_selection_str = f"!{ligand_selection_str}"
    
    ligand_structure = full_structure[ligand_selection_str]
    ligand_structure.save(str(ligand_filename), overwrite=True)
    nonligand_structure = full_structure[receptor_selection_str]
    nonligand_structure.save(str(receptor_filename), overwrite=True)
    return

def _make_ligand_sdf_file(
        seekrflow: structures.Seekrflow,
        ) -> None:
    """
    Make an SDF file for the ligand from the PDB file.
    """
    #param_dir = seekrflow.get_parametrize_directory()
    ligand_pdb_filename = os.path.join(structures.PARAMETRIZE, LIGAND_PDB_FILENAME)
    ligand_sdf_filename = os.path.join(structures.PARAMETRIZE, DEFAULT_LIGAND_SDF_FILENAME)
    seekrflow.ligand_sdf_file = str(ligand_sdf_filename)
    assert os.path.exists(ligand_pdb_filename), \
        f"Ligand PDB file {ligand_pdb_filename} does not exist."
    # Create input and output molecule streams
    from openeye import oechem
    ifs = oechem.oemolistream()
    ofs = oechem.oemolostream()
    # Open input PDB file and output SDF file
    if not ifs.open(str(ligand_pdb_filename)):
        oechem.OEThrow.Fatal(f"Unable to open the input PDB file: {ligand_pdb_filename}")
    if not ofs.open(seekrflow.ligand_sdf_file):
        oechem.OEThrow.Fatal(f"Unable to create the output SDF file: {seekrflow.ligand_sdf_file}")
    # Convert PDB to SDF using a generator for reading molecules
    for mol in ifs.GetOEGraphMols():
        oechem.OEWriteMolecule(ofs, mol)
    # Close the streams
    ifs.close()
    ofs.close()
    return

def choose_only_receptor_atoms(
        complex_structure: parmed.Structure, 
        receptor_structure: parmed.Structure
        ) -> parmed.Structure:
    """
    If the complex_structure file contains the atomic charges and radii,
    conversely the receptor_structure file contains all atoms of the receptor,
    then select from complex_structure the atoms that will be present in the
    receptor_structure file and place them into a new parmed Structure
    """
    chain_ids = set()
    for atom in receptor_structure.atoms:
        if atom.residue.chain is not None:
            chain = atom.residue.chain
            chain_ids.add(chain)
    receptor = complex_structure[",".join(chain_ids), :, :]
    return receptor

def _make_pqr_files(
        seekrflow: structures.Seekrflow,
        parmed_complex: parmed.Structure,
        ) -> None:
    """
    Make PQR files for the receptor and ligand.
    """
    #param_dir = seekrflow.get_parametrize_directory()
    # Assign mbondi2 radii to atoms
    for atom in parmed_complex.atoms:
        if atom.element == 1:
            for bonded_atom in atom.bond_partners:
                if bonded_atom.element == 7:
                    atom.solvent_radius = 1.3
                else:
                    atom.solvent_radius = 1.2
        elif atom.element == 6:
            atom.solvent_radius = 1.7
        elif atom.element == 7:
            atom.solvent_radius = 1.55
        elif atom.element == 8:
            atom.solvent_radius = 1.5
        elif atom.element == 9:
            atom.solvent_radius = 1.5
        elif atom.element == 14:
            atom.solvent_radius = 2.1
        elif atom.element == 15:
            atom.solvent_radius = 1.85
        elif atom.element == 16:
            atom.solvent_radius = 1.8
        elif atom.element == 17:
            atom.solvent_radius = 1.7
        else:
            atom.solvent_radius = 1.7
    
    # First, handle the ligand
    ligand_first_index = seekrflow.ligand_indices[0]
    ligand_first_atom = parmed_complex.atoms[ligand_first_index]
    ligand_first_atom_resname = ligand_first_atom.residue.name
    ligand_structure_all_resids = parmed_complex[f":{ligand_first_atom_resname}"]
    last_resnum = None
    last_chain = None
    atom_number_list = []
    for j, atom in enumerate(ligand_structure_all_resids.atoms):
        if last_resnum is None:
            last_resnum = atom.residue.number
        if last_chain is None:
            last_chain = atom.residue.chain
        if (atom.residue.number != last_resnum) or (atom.residue.chain != last_chain):
            break
        atom_number_list.append(j)
    ligand = ligand_structure_all_resids[:len(atom_number_list)]

    # Next, handle the receptor
    not_ligand = parmed_complex[f"!:{ligand_first_atom_resname}"]
    not_ligand_tmp_filename = os.path.join(structures.PARAMETRIZE, "not_ligand_tmp.pdb")
    not_ligand.save(not_ligand_tmp_filename, overwrite=True)
    fixer_settings = structures.PDBFixer_settings(
        remove_extra_chains=True,
        find_missing_residues=False,
        find_and_replace_nonstandard_residues=False,
        find_and_add_missing_atoms=False,
        add_missing_hydrogens_pH=False,
    )
    receptor_tmp_filename = os.path.join(structures.PARAMETRIZE, "receptor_tmp.pdb")
    fixer_settings.run(not_ligand_tmp_filename, receptor_tmp_filename)
    full_receptor_pqr_filename = os.path.join(structures.PARAMETRIZE, "receptor.pqr")
    receptor_tmp_structure = parmed.load_file(
        receptor_tmp_filename)
    receptor = choose_only_receptor_atoms(parmed_complex, receptor_tmp_structure)
    
    full_ligand_pqr_filename_one_resid = os.path.join(structures.PARAMETRIZE, "ligand_one_resid.pqr")
    receptor.box = None  # Set box vectors to None for PQR
    ligand.box = None  # Set box vectors to None for PQR
    receptor.save(full_receptor_pqr_filename, overwrite=True)
    ligand.save(full_ligand_pqr_filename_one_resid, overwrite=True)

    full_ligand_pqr_filename = os.path.join(structures.PARAMETRIZE, "ligand.pqr")
    pqr_resid_for_each_atom.pqr_resid_for_each_atom(
        full_ligand_pqr_filename_one_resid, full_ligand_pqr_filename)

    return full_receptor_pqr_filename, full_ligand_pqr_filename

def _regenerate_espaloma_system(
        seekrflow: structures.Seekrflow,
        system_generator: SystemGenerator,
        solvated_topology: openmm.app.Topology,
        solvated_positions: openmm.unit.Quantity,
        ) -> openmm.System:
    """
    Add espaloma parameterization to the solvated system.
    """
    #param_dir = seekrflow.get_parametrize_directory()
    output_pdb_basename = os.path.join(structures.PARAMETRIZE, seekrflow.basename_output)
    # Convert solvated topology to MDTraj format and identify protein chains
    mdtop = mdtraj.Topology.from_openmm(solvated_topology)
    chain_indices = [chain.index for chain in solvated_topology.chains()]
    protein_chain_indices \
        = [chain_index for chain_index in chain_indices if mdtop.select(
            f"protein and chainid == {chain_index}").any()]
    # Create new topology and copy chains
    new_solvated_topology = openmm_app.Topology()
    new_solvated_topology.setPeriodicBoxVectors(solvated_topology.getPeriodicBoxVectors())
    new_atoms = {}
    chain_counter = 0
    for chain in solvated_topology.chains():
        new_chain = new_solvated_topology.addChain(chain.id)
        if chain.index in protein_chain_indices:
            resname = f'XX{chain_counter:01d}'  # Assign unique residue name for each protein chain
            resid = '1'
            chain_counter += 1
            new_residue = new_solvated_topology.addResidue(resname, new_chain, resid)
        for residue in chain.residues():
            if residue.chain.index not in protein_chain_indices:
                new_residue = new_solvated_topology.addResidue(residue.name, new_chain, residue.id)
            for atom in residue.atoms():
                new_atom = new_solvated_topology.addAtom(atom.name, atom.element, new_residue, atom.id)
                new_atoms[atom] = new_atom
    for bond in solvated_topology.bonds():
        if bond[0] in new_atoms and bond[1] in new_atoms:
            new_solvated_topology.addBond(new_atoms[bond[0]], new_atoms[bond[1]])
    # Save the complex with ESPALOMA parameterization to PDB file
    complex_espaloma_filename = f"{output_pdb_basename}-espaloma-bad-resids.pdb"
    openmm_app.PDBFile.writeFile(new_solvated_topology, solvated_positions, 
                                 file=open(complex_espaloma_filename, 'w'))
    # Split protein chains into separate PDB files
    protein_espaloma_filenames = []
    for chain_index in protein_chain_indices:
        t = mdtraj.load_pdb(complex_espaloma_filename)
        indices = t.topology.select(f"chainid == {chain_index}")
        chain_espaloma_filename = f"{output_pdb_basename}-espaloma-{chain_index}.pdb"
        t.atom_slice(indices).save_pdb(chain_espaloma_filename)
        protein_espaloma_filenames.append(chain_espaloma_filename)

    # Load protein molecules and add to system generator template
    protein_molecules = [Molecule.from_file(protein_filename) for protein_filename in protein_espaloma_filenames]
    system_generator.template_generator.add_molecules(protein_molecules)
    # Create new solvated system
    new_solvated_system = system_generator.create_system(new_solvated_topology)
    return new_solvated_system

def _create_complex(
        seekrflow: structures.Seekrflow
        ) -> typing.Tuple[str, str]:
    """
    Create the complex structure.
    """
    #param_dir = seekrflow.get_parametrize_directory()
    receptor_filename = os.path.join(structures.PARAMETRIZE, RECEPTOR_PDB_FILENAME)
    ligand_filename = os.path.join(structures.PARAMETRIZE, LIGAND_PDB_FILENAME)
    assert os.path.exists(receptor_filename), \
        f"Receptor PDB file {receptor_filename} does not exist."
    assert os.path.exists(ligand_filename), \
        f"Ligand PDB file {ligand_filename} does not exist."
    apo_pdb_filename_no_ext = os.path.join(structures.PARAMETRIZE, "receptor")
    latest_pdb_filename = str(receptor_filename)
    if seekrflow.pdb_fixer_settings is not None:
        fixed_pdb_filename = str(apo_pdb_filename_no_ext) + "_fixed.pdb"
        seekrflow.pdb_fixer_settings.run(latest_pdb_filename, fixed_pdb_filename)
        latest_pdb_filename = fixed_pdb_filename
    if seekrflow.pdb2pqr_settings is not None:
        pdb2pqr_output_pqr_filename = str(apo_pdb_filename_no_ext) + "_pdb2pqr.pqr"
        pdb2pqr_output_pdb_filename = str(apo_pdb_filename_no_ext) + "_pdb2pqr.pdb"
        seekrflow.pdb2pqr_settings.run(latest_pdb_filename, pdb2pqr_output_pqr_filename,
                                       pdb2pqr_output_pdb_filename)
        latest_pdb_filename = pdb2pqr_output_pdb_filename

    # Load the receptor
    with open(latest_pdb_filename, 'r') as f:
        protein = openmm_app.PDBFile(f) 
    protein_topology = protein.topology
    protein_positions = protein.positions 
    protein_md_topology = mdtraj.Topology.from_openmm(protein_topology)

    # Load the ligand
    assert os.path.splitext(seekrflow.ligand_sdf_file)[-1].lower(), \
        "Ligand SDF file must have a .sdf extension."
    suppl = Chem.SDMolSupplier(seekrflow.ligand_sdf_file)
    mols = [x for x in suppl if x is not None]
    mol = mols[0]
    img = Draw.MolToImage(mol, size=(300, 300))
    img.save(os.path.join(structures.PARAMETRIZE,  "ligand.png"))
    ligand_pdb = parmed.load_file(str(ligand_filename), skip_bonds=True)
    ligand_positions_in_A_pdb = ligand_pdb.coordinates
    conf = mol.GetConformer()
    assert ligand_positions_in_A_pdb.shape[0] == mol.GetNumAtoms()
    for i in range(mol.GetNumAtoms()):
        conf.SetAtomPosition(i, Point3D(ligand_positions_in_A_pdb[i,0],
                                        ligand_positions_in_A_pdb[i,1],
                                        ligand_positions_in_A_pdb[i,2]))
    
    rdForceFieldHelpers.MMFFOptimizeMolecule(mol)
    offmol = Molecule.from_rdkit(mol)
    ligand_positions = to_openmm(offmol.conformers[0])
    ligand_topology = offmol.to_topology().to_openmm()
    output_pdb_basename = os.path.join(structures.PARAMETRIZE, seekrflow.basename_output)
    output_pdb_filename_ligand = f"{output_pdb_basename}_ligand.pdb"
    openmm_app.PDBFile.writeFile(ligand_topology, ligand_positions.value_in_unit(unit.nanometers), 
                                 file=open(output_pdb_filename_ligand, 'w'))
    ligand_md_topology = mdtraj.Topology.from_openmm(ligand_topology)
    for atom in ligand_md_topology.atoms:
        if seekrflow.ligand_resname == "":
            atom.residue.name = "LIG"
        else:
            atom.residue.name = seekrflow.ligand_resname
        
    complex_md_topology = protein_md_topology.join(ligand_md_topology)
    complex_topology = complex_md_topology.to_openmm()
    # Ensure the number of atoms is consistent after merging
    n_atoms_total = complex_md_topology.n_atoms
    n_atoms_protein = protein_md_topology.n_atoms
    n_atoms_ligand = ligand_md_topology.n_atoms
    assert n_atoms_total == n_atoms_protein + n_atoms_ligand, "Mismatch in atom numbers after merging."
    complex_positions = np.zeros([n_atoms_total, 3]) * unit.nanometers
    complex_positions[:n_atoms_protein, :] = protein_positions
    complex_positions[n_atoms_protein:n_atoms_protein + n_atoms_ligand, :] = ligand_positions
    modeller = openmm_app.Modeller(complex_topology, complex_positions)
    nonsolvated_topology = modeller.getTopology()
    nonsolvated_positions = modeller.getPositions()

    forcefield_kwargs = {'removeCMMotion': True, 
                         'ewaldErrorTolerance': PME_TOL, 
                         'constraints': openmm_app.HBonds, 
                         'rigidWater': True, 
                         'hydrogenMass': seekrflow.hmass * unit.amu}
    periodic_forcefield_kwargs = {'nonbondedMethod': openmm_app.PME}
    if seekrflow.pressure is not None:
        barostat = openmm.MonteCarloBarostat(seekrflow.pressure * unit.atmosphere, 
                                             seekrflow.temperature * unit.kelvin, 
                                             seekrflow.barostat_period)
    else:
        barostat = None
    
    # TODO: these need to be set in the seekrflow object
    FF_FILES = [
        "amber/ff14SB.xml",
        "amber/tip3p_standard.xml",
        "amber/tip3p_HFE_multivalent.xml"
    ]
    
    system_generator = SystemGenerator(
        forcefields=FF_FILES, 
        forcefield_kwargs=forcefield_kwargs, 
        periodic_forcefield_kwargs=periodic_forcefield_kwargs, 
        barostat=barostat, 
        small_molecule_forcefield=seekrflow.parametrizer.forcefield, 
        molecules=offmol, 
        cache=None)
    nonsolvated_system = system_generator.create_system(nonsolvated_topology)
    integrator = openmm.LangevinMiddleIntegrator(
        seekrflow.temperature * unit.kelvin, 
        seekrflow.friction / unit.picosecond, 
        seekrflow.stepsize * unit.picoseconds)
    simulation = openmm_app.Simulation(nonsolvated_topology, nonsolvated_system, 
                                       integrator)
    simulation.context.setPositions(nonsolvated_positions)
    simulation.minimizeEnergy(maxIterations=MAX_MINIMIZATION_ITERATIONS)
    state = simulation.context.getState(getPositions=True, enforcePeriodicBox=True)
    nonsolvated_positions_minimized = state.getPositions()
    output_pdb_filename_nosolv = f"{output_pdb_basename}_no_solvent.pdb"
    openmm_app.PDBFile.writeFile(nonsolvated_topology, nonsolvated_positions_minimized, 
                                 file=open(output_pdb_filename_nosolv, 'w'))
    # TODO: set up possibility for triclinic water box
    solv_modeller = openmm_app.Modeller(nonsolvated_topology, nonsolvated_positions_minimized)
    solv_modeller.addSolvent(
        system_generator.forcefield, 
        model=seekrflow.parametrizer.water_model, 
        padding=seekrflow.solvent_padding * unit.nanometers, 
        ionicStrength=seekrflow.ionic_strength * unit.molar)
    solvated_topology = solv_modeller.getTopology()
    solvated_positions = solv_modeller.getPositions()
    solvated_system = system_generator.create_system(solvated_topology)
    
    #output_pdb_filename = f"{output_pdb_basename}.pdb"
    #openmm_app.PDBFile.writeFile(solvated_topology, solvated_positions, file=open(output_pdb_filename, 'w'))
    
    serialized_xml = f"{output_pdb_basename}.xml"
    if seekrflow.parametrizer.forcefield.startswith("espaloma"):
        new_system = _regenerate_espaloma_system(
            seekrflow,
            system_generator,
            solvated_topology,
            solvated_positions)
    else:
        new_system = solvated_system
    
    with open(serialized_xml, "w") as wf:
        xml = openmm.XmlSerializer.serialize(new_system)  # Serialize system
        wf.write(xml)
    
    integrator = openmm.LangevinMiddleIntegrator(
        seekrflow.temperature * unit.kelvin, 
        seekrflow.friction / unit.picosecond, 
        seekrflow.stepsize * unit.picoseconds)
    simulation = openmm_app.Simulation(solvated_topology, solvated_system, integrator)
    simulation.context.setPositions(solvated_positions)
    simulation.minimizeEnergy(maxIterations=MAX_MINIMIZATION_ITERATIONS)
    simulation.step(1000)
    state = simulation.context.getState(getPositions=True, enforcePeriodicBox=True)
    solvated_positions_equilibrated = state.getPositions()
    output_pdb_filename = f"{output_pdb_basename}-equil.pdb"
    openmm_app.PDBFile.writeFile(solvated_topology, solvated_positions_equilibrated, file=open(output_pdb_filename, 'w'))
    
    return serialized_xml, output_pdb_filename

def parametrize(
    seekrflow: structures.Seekrflow,
    ) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
    """
    Given a Seekrflow object including a  PDB file receptor-ligand complex 
    as a starting structure, attempt to parametrize the file based on input 
    parameters.
    """
    # TODO: output warning message about parametrizing your own systems and checking all
    #  results.
    assert seekrflow.workflow_type == "protein_ligand", \
        "Only protein-ligand workflows are supported at this time for parametrize.py"
    param_dir = seekrflow.get_parametrize_directory()
    curdir = os.getcwd()
    assert seekrflow.receptor_ligand_pdb != "", \
        "seekrflow.receptor_ligand_pdb is not set, cannot parametrize."
    work_copy_receptor_ligand_pdb = os.path.join(
        seekrflow.work_directory, os.path.basename(seekrflow.receptor_ligand_pdb))
    copyfile(seekrflow.receptor_ligand_pdb, work_copy_receptor_ligand_pdb)
    if seekrflow.ligand_sdf_file != "":
        work_copy_ligand_sdf = os.path.join(
            seekrflow.work_directory, DEFAULT_LIGAND_SDF_FILENAME)
        assert os.path.exists(seekrflow.ligand_sdf_file), \
            f"Ligand SDF file {seekrflow.ligand_sdf_file} does not exist."
        copyfile(seekrflow.ligand_sdf_file, work_copy_ligand_sdf)
    os.chdir(seekrflow.work_directory)
    _split_receptor_ligand(seekrflow)
    if seekrflow.ligand_sdf_file == "":
        _make_ligand_sdf_file(seekrflow)

    if seekrflow.parametrizer is None:
        # TODO: logger?
        print("seekrflow.parametrizer is None, Skipping full parametrization.")
        if (seekrflow.md_parameters_topology is not None) \
                and (seekrflow.starting_pdb_filename != ""):
            parmed_complex = seekrflow.md_parameters_topology.make_parmed(
                seekrflow.starting_pdb_filename, seekrflow.work_directory)
            if seekrflow.bd_settings is not None:
                full_receptor_pqr_filename, full_ligand_pqr_filename\
                    = _make_pqr_files(seekrflow, parmed_complex)
                receptor_pqr_basename = os.path.basename(full_receptor_pqr_filename)
                ligand_pqr_basename = os.path.basename(full_ligand_pqr_filename)
                seekrflow.bd_settings.receptor_pqr_filename = os.path.join(
                    structures.PARAMETRIZE, receptor_pqr_basename)
                seekrflow.bd_settings.ligand_pqr_filename = os.path.join(
                    structures.PARAMETRIZE, ligand_pqr_basename)
        os.chdir(curdir)
        return None, None
    else:
        serialized_xml, output_pdb_filename = _create_complex(seekrflow)
        starting_pdb_basename = os.path.basename(output_pdb_filename)
        seekrflow.starting_pdb_filename = os.path.join(structures.PARAMETRIZE, starting_pdb_basename)
        system_basename = os.path.basename(serialized_xml)
        seekrflow.md_parameters_topology = structures.Openmm_system(
            system_filename=os.path.join(structures.PARAMETRIZE, system_basename),)
        parmed_complex = seekrflow.md_parameters_topology.make_parmed(
            output_pdb_filename)
        if seekrflow.bd_settings is not None:
            full_receptor_pqr_filename, full_ligand_pqr_filename\
                = _make_pqr_files(seekrflow, parmed_complex)
            receptor_pqr_basename = os.path.basename(full_receptor_pqr_filename)
            ligand_pqr_basename = os.path.basename(full_ligand_pqr_filename)
            seekrflow.bd_settings.receptor_pqr_filename = os.path.join(
                structures.PARAMETRIZE, receptor_pqr_basename)
            seekrflow.bd_settings.ligand_pqr_filename = os.path.join(
                structures.PARAMETRIZE, ligand_pqr_basename)
        
        os.chdir(curdir)
        return serialized_xml, output_pdb_filename

def main() -> None:
    argparser = argparse.ArgumentParser(
        description="Given a PDB file receptor-ligand complex as a starting structure, "
        "attempt to parametrize the file based on some input parameters.")
    argparser.add_argument(
        "receptor_ligand_pdb", metavar="RECEPTOR_LIGAND_PDB", type=str, 
        help="Path to the input PDB receptor_ligand file.")
    argparser.add_argument(
        "-i", "--input_json", dest="input_json",
        metavar="INPUT_JSON", type=str, default="",
        help="Path to the input JSON file containing the parameters for "\
        "the parametrization. Default is empty, which will use the default "\
        "seekrflow parameters.")
    argparser.add_argument(
        "-s", "--ligand_sdf_file", dest="ligand_sdf_file",
        metavar="LIGAND_SDF_FILE", type=str, default="",
        help="Optional path to the input SDF file containing the ligand molecule ")
    argparser.add_argument(
        "-L", "--ligand_resname", dest="ligand_resname", 
        metavar="LIGAND_RESNAME", type=str, default="",
        help="The residue name of the ligand molecule for automatic index "\
        "selection.")
    argparser.add_argument(
        "-l", "--ligand_indices", dest="ligand_indices", 
        metavar="LIGAND_INDICES", type=str, default="",
        help="A comma-separated list of integers defining site within the "\
        "ref_pdb structure.")
    argparser.add_argument(
        "-w", "--work_directory", dest="work_directory",
        metavar="WORK_DIRECTORY", type=str, default=structures.WORK,
        help="Path to the work directory for the parametrization.")
    argparser.add_argument(
        "-b", "--basename_output", dest="basename_output",
        metavar="BASENAME_OUTPUT", type=str, default="complex",
        help="The basename of the output files. Default is 'complex'.")
    argparser.add_argument(
        "-e", "--external_ff_file", dest="external_ff_file",
        metavar="EXTERNAL_FF_FILE", type=str, default=None,
        help="Any external force field file to use for the parametrization. "\
            "For example, 'espaloma-0.3.2.pt' can be entered here.")
    # TODO: add argument that can modify any attr of the Seekrflow object
    # then, use set_attr to set the value in the Seekrflow object
    args = argparser.parse_args()
    args = vars(args)
    receptor_ligand_pdb = pathlib.Path(args["receptor_ligand_pdb"])
    assert receptor_ligand_pdb.exists(), \
        f"Input PDB file {receptor_ligand_pdb} does not exist."
    input_json = args["input_json"]
    ligand_sdf_file = args["ligand_sdf_file"]
    ligand_indices = args["ligand_indices"]
    ligand_resname = args["ligand_resname"]
    
    # TODO: create and perform work within subdirectories of the work/ directory,
    #  like work/parametrize/ , etc.
    work_dir = pathlib.Path(args["work_directory"])

    # TODO: load the input JSON file structure
    if input_json != "":
        if not os.path.exists(input_json):
            raise FileNotFoundError(f"Input JSON file {input_json} does not exist.")
        seekrflow = structures.load_seekrflow(input_json)
    else:
        seekrflow = structures.Seekrflow()
    
    if ligand_resname == "":
        ligand_resname = seekrflow.ligand_resname
        
    if ligand_indices != "":
        ligand_indices = base.initialize_ref_indices(ligand_indices)
    else:
        if ligand_resname != "":
            ligand_indices = base.get_ligand_indices(receptor_ligand_pdb, ligand_resname)
        else:
            ligand_indices = []

    seekrflow.make_work_directory(work_dir)
    seekrflow.basename_output = args["basename_output"]

    external_ff_file = args["external_ff_file"]
    if external_ff_file is not None:
        if not os.path.exists(external_ff_file):
            raise FileNotFoundError(f"External force field file {external_ff_file} does not exist.")
        
        seekrflow.parametrizer.forcefield = external_ff_file
        #seekrflow.parametrizer.forcefield = "~/Downloads/espaloma-0.3.2.pt"

    # For now, the receptor_ligand_pdb is required, so we set it here.``
    seekrflow.receptor_ligand_pdb = str(receptor_ligand_pdb)
    if ligand_sdf_file != "":
        assert os.path.exists(ligand_sdf_file), \
            "Nonexistent ligand SDF file: {}".format(ligand_sdf_file)
        # TODO: copy over the SDF file to the output directory if it exists
        seekrflow.ligand_sdf_file = ligand_sdf_file
    if len(ligand_indices) > 0:
        seekrflow.ligand_indices = ligand_indices
        seekrflow.ligand_resname = ligand_resname
    else:
        if seekrflow.ligand_indices != "":
            seekrflow.ligand_indices = base.initialize_ref_indices(seekrflow.ligand_indices)
        else:
            if seekrflow.ligand_resname != "":
                seekrflow.ligand_indices = base.get_ligand_indices(receptor_ligand_pdb, seekrflow.ligand_resname)
            else:
                raise Exception("No ligand indices provided and no ligand residue name specified.")

    system_filename, positions_filename = parametrize(seekrflow)
    seekrflow.work_directory = str(work_dir)
    structures.save_new_seekrflow(seekrflow, PARAM_SEEKRFLOW_GLOB, PARAM_SEEKRFLOW_BASE, 
                                  directory=seekrflow.work_directory)
        
    return

if __name__ == "__main__":
    main()