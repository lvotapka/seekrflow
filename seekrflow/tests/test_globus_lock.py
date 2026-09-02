"""Tests for the batch Globus Compute one-at-a-time lock."""

from __future__ import annotations

import os

import seekrflow.modules.batch.globus_lock as globus_lock


def test_globus_lock_noop_when_path_none():
    assert globus_lock.try_acquire(None, "submit") is True
    assert globus_lock.acquire_blocking(None, "cancel") is True
    globus_lock.release(None)


def test_globus_lock_cancel_beats_submit_beats_status(tmp_path, monkeypatch):
    monkeypatch.setattr(globus_lock, "_pid_alive", lambda pid: True)
    path = str(tmp_path / globus_lock.GLOBUS_LOCK_FILENAME)
    globus_lock.init_lock(path)
    assert globus_lock.try_acquire(path, "status", pid=1, tid=1)
    assert not globus_lock.try_acquire(path, "submit", pid=2, tid=2)
    assert not globus_lock.try_acquire(path, "status_focused", pid=3, tid=3)
    assert not globus_lock.try_acquire(path, "cancel", pid=4, tid=4)
    globus_lock.release(path, pid=1, tid=1)
    assert globus_lock.try_acquire(path, "cancel", pid=4, tid=4)
    assert not globus_lock.try_acquire(path, "submit", pid=2, tid=2)
    globus_lock.release(path, pid=4, tid=4)
    assert globus_lock.try_acquire(path, "submit", pid=2, tid=2)
    assert not globus_lock.try_acquire(path, "status_focused", pid=3, tid=3)
    globus_lock.release(path, pid=2, tid=2)
    assert globus_lock.try_acquire(path, "status_focused", pid=3, tid=3)


def test_globus_lock_oldest_wins_same_priority(tmp_path, monkeypatch):
    monkeypatch.setattr(globus_lock, "_pid_alive", lambda pid: True)
    path = str(tmp_path / globus_lock.GLOBUS_LOCK_FILENAME)
    globus_lock.init_lock(path)
    assert globus_lock.try_acquire(path, "status", pid=1, tid=1)
    assert not globus_lock.try_acquire(path, "submit", pid=2, tid=2)
    assert not globus_lock.try_acquire(path, "submit", pid=3, tid=3)
    globus_lock.release(path, pid=1, tid=1)
    assert globus_lock.try_acquire(path, "submit", pid=2, tid=2)
    assert not globus_lock.try_acquire(path, "submit", pid=3, tid=3)


def test_globus_lock_reaps_dead_holder(tmp_path, monkeypatch):
    path = str(tmp_path / globus_lock.GLOBUS_LOCK_FILENAME)
    globus_lock.init_lock(path)
    monkeypatch.setattr(globus_lock, "_pid_alive", lambda pid: pid != 999)
    assert globus_lock.try_acquire(path, "submit", pid=999, tid=1)
    assert globus_lock.try_acquire(path, "status", pid=1, tid=2)


def test_duplicate_status_key_is_skipped(tmp_path, monkeypatch):
    monkeypatch.setattr(globus_lock, "_pid_alive", lambda pid: True)
    path = str(tmp_path / globus_lock.GLOBUS_LOCK_FILENAME)
    globus_lock.init_lock(path)
    key = "ep|/root/a|mmvt"
    assert globus_lock.try_begin_in_flight(
        path, "status", key, pid=1) == globus_lock.IN_FLIGHT_SUBMIT
    assert globus_lock.try_begin_in_flight(
        path, "status", key, pid=2) == globus_lock.IN_FLIGHT_DUPLICATE
    assert globus_lock.try_begin_in_flight(
        path, "status_focused", key, pid=3) == globus_lock.IN_FLIGHT_DUPLICATE
    globus_lock.end_in_flight(path, key, pid=1)
    assert globus_lock.try_begin_in_flight(
        path, "status", key, pid=2) == globus_lock.IN_FLIGHT_SUBMIT


def test_status_in_flight_cap_is_two(tmp_path, monkeypatch):
    monkeypatch.setattr(globus_lock, "_pid_alive", lambda pid: True)
    path = str(tmp_path / globus_lock.GLOBUS_LOCK_FILENAME)
    globus_lock.init_lock(path)
    k1 = "ep|/root/a|mmvt"
    k2 = "ep|/root/b|mmvt"
    k3 = "ep|/root/c|mmvt"
    assert globus_lock.try_begin_in_flight(
        path, "status", k1, pid=1) == globus_lock.IN_FLIGHT_SUBMIT
    assert globus_lock.try_begin_in_flight(
        path, "status_focused", k2, pid=2) == globus_lock.IN_FLIGHT_SUBMIT
    assert globus_lock.try_begin_in_flight(
        path, "status", k3, pid=3) == globus_lock.IN_FLIGHT_AT_CAPACITY
    # Submit/cancel are not capped this way.
    assert globus_lock.try_begin_in_flight(
        path, "submit", k3, pid=4) == globus_lock.IN_FLIGHT_SUBMIT
    globus_lock.end_in_flight(path, k1, pid=1)
    assert globus_lock.try_begin_in_flight(
        path, "status", k3, pid=3) == globus_lock.IN_FLIGHT_SUBMIT


def test_in_flight_reaps_dead_pid(tmp_path, monkeypatch):
    path = str(tmp_path / globus_lock.GLOBUS_LOCK_FILENAME)
    globus_lock.init_lock(path)
    monkeypatch.setattr(globus_lock, "_pid_alive", lambda pid: pid != 1)
    key = "ep|/root/a|mmvt"
    assert globus_lock.try_begin_in_flight(
        path, "status", key, pid=1) == globus_lock.IN_FLIGHT_SUBMIT
    # Dead pid is reaped, so the same key can be submitted again.
    assert globus_lock.try_begin_in_flight(
        path, "status", key, pid=2) == globus_lock.IN_FLIGHT_SUBMIT


def test_acquire_release_preserves_in_flight(tmp_path, monkeypatch):
    monkeypatch.setattr(globus_lock, "_pid_alive", lambda pid: True)
    path = str(tmp_path / globus_lock.GLOBUS_LOCK_FILENAME)
    globus_lock.init_lock(path)
    key = "ep|/root/a|mmvt"
    assert globus_lock.try_begin_in_flight(
        path, "status", key, pid=10) == globus_lock.IN_FLIGHT_SUBMIT
    assert globus_lock.try_acquire(path, "submit", pid=1, tid=1)
    globus_lock.release(path, pid=1, tid=1)
    assert globus_lock.try_begin_in_flight(
        path, "status", key, pid=11) == globus_lock.IN_FLIGHT_DUPLICATE


def test_globus_lock_env_name():
    assert globus_lock.LOCK_ENV == "SEEKR_GLOBUS_LOCK_FILE"
    assert globus_lock.lock_file_path("/tmp/batch").endswith(
        globus_lock.GLOBUS_LOCK_FILENAME)
    assert os.path.basename(globus_lock.lock_file_path("/tmp/batch")) == (
        ".seekrflow_globus_lock.json")
