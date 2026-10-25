"""The command-line status checks, when the server is found by its port alone."""
import os

import psutil

import main


def test_running_server_without_a_pid_file_is_reported_by_its_port(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(main, "PID_FILE", tmp_path / "news_server.pid")  # never written
    monkeypatch.setattr(main, "is_server_running", lambda: True)
    monkeypatch.setattr(main, "get_process_using_port", lambda port: psutil.Process(os.getpid()))

    main.start_server()
    main.check_server()

    out = capsys.readouterr().out
    assert f"already running with PID {os.getpid()}" in out
    assert f"PID: {os.getpid()}" in out


def test_a_port_held_by_a_process_we_cannot_inspect_still_reports_running(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(main, "PID_FILE", tmp_path / "news_server.pid")
    monkeypatch.setattr(main, "is_server_running", lambda: True)
    monkeypatch.setattr(main, "get_process_using_port", lambda port: None)

    main.check_server()

    assert "PID: unknown" in capsys.readouterr().out
