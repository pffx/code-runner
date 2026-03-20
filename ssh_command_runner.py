#!/usr/bin/env python3

# Enable postponed evaluation of type annotations.
from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import re
import socket
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import paramiko


def get_base_dir() -> Path:
    # When packaged as a one-file executable, read sibling config files next to the executable.
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    # In normal script mode, use the script directory.
    return Path(__file__).resolve().parent


# The working base directory for default input/output paths.
BASE_DIR = get_base_dir()
# Default device configuration path.
DEFAULT_DEVICE_FILE = BASE_DIR / "device.json"
# Default commands file path.
DEFAULT_COMMANDS_FILE = BASE_DIR / "commands.txt"
# Default runtime log file path.
DEFAULT_LOG_FILE = BASE_DIR / "log" / "run.log"
# Default failed devices report file path.
DEFAULT_FAILED_DEVICES_FILE = BASE_DIR / "log" / "failed_devices.log"

# Match CSI-style ANSI escape sequences, like ESC[1D.
ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
# Match other short ESC control sequences.
OTHER_ESCAPE_RE = re.compile(r"\x1B[@-_]")
# Minimum timeout threshold in seconds (must be greater than 2 minutes).
MIN_COMMAND_TIMEOUT_SECONDS = 121


def parse_args() -> argparse.Namespace:
    # Build the top-level argument parser.
    parser = argparse.ArgumentParser(
        # Brief CLI description.
        description="SSH login to a network device and run commands from commands.txt"
    )
    # Optional path to device json file.
    parser.add_argument(
        # Device file flag.
        "--device-file",
        # Use local default device config file.
        default=str(DEFAULT_DEVICE_FILE),
        # Help text for users.
        help="Path to device.json",
    )
    # Optional override for host in single-device mode.
    parser.add_argument("--host", help="Network device IP or hostname")
    # Optional override for SSH port.
    parser.add_argument("--port", type=int, help="SSH port, default 22")
    # Optional override for username.
    parser.add_argument("--username", help="SSH username")
    # Optional override for expected device prompt text.
    parser.add_argument("--prompt", help="Expected prompt text used to detect command completion")
    # Optional override for password.
    parser.add_argument(
        # Password flag.
        "--password",
        # Fallback to SSH_PASSWORD env variable.
        default=os.getenv("SSH_PASSWORD"),
        # Help text for password source.
        help="SSH password. If omitted, device.json or SSH_PASSWORD is used.",
    )
    # Optional path to commands list file.
    parser.add_argument(
        # Commands file flag.
        "--commands-file",
        # Use local default commands file.
        default=str(DEFAULT_COMMANDS_FILE),
        # Help text for commands file.
        help="Path to commands.txt",
    )
    # Optional explicit result file path for single-device mode.
    parser.add_argument(
        # Result file flag.
        "--result-file",
        # Help text for explicit result file behavior.
        help="Explicit result log path. If omitted, a file named <host>_result_<timestamp>.log is created in the script directory.",
    )
    # Optional runtime log file path.
    parser.add_argument(
        # Log file flag.
        "--log-file",
        # Use local default runtime log file.
        default=str(DEFAULT_LOG_FILE),
        # Help text for log file path.
        help="Path to run.log",
    )
    # SSH connect timeout per device.
    parser.add_argument(
        # Connect timeout flag.
        "--connect-timeout",
        # Parse timeout as integer seconds.
        type=int,
        # Default connect timeout.
        default=10,
        # Help text for connect timeout.
        help="SSH connection timeout in seconds",
    )
    # Max wait per command output collection.
    parser.add_argument(
        # Command timeout flag.
        "--command-timeout",
        # Parse timeout as integer seconds.
        type=int,
        # Default command timeout (>2 minutes).
        default=130,
        # Help text for command timeout.
        help="Maximum wait time for each command in seconds. Must be greater than 120.",
    )
    # Idle wait threshold to decide output is complete.
    parser.add_argument(
        # Command interval flag.
        "--command-interval",
        # Parse interval as float seconds.
        type=float,
        # Default idle interval.
        default=0.8,
        # Help text for command interval.
        help="Idle wait after command output settles in seconds",
    )
    # Optional cap for concurrent workers.
    parser.add_argument(
        # Max workers flag.
        "--max-workers",
        # Parse workers as integer.
        type=int,
        # Help text for parallel workers.
        help="Maximum number of devices to execute in parallel. Defaults to the number of devices.",
    )
    # Per-device total execution timeout.
    parser.add_argument(
        # Session timeout flag.
        "--session-timeout",
        # Parse timeout as integer seconds.
        type=int,
        # Default per-device timeout (10 minutes).
        default=600,
        # Help text for per-device timeout.
        help="Maximum total execution time per device in seconds.",
    )
    # Retry attempts per device for transient failures.
    parser.add_argument(
        # Retry count flag.
        "--retries",
        # Parse retries as integer.
        type=int,
        # Default retry attempts.
        default=3,
        # Help text for retries.
        help="Retry attempts per device for transient failures.",
    )
    # Optional failed devices report path.
    parser.add_argument(
        # Failed devices report flag.
        "--failed-devices-file",
        # Default failed devices report file.
        default=str(DEFAULT_FAILED_DEVICES_FILE),
        # Help text for failed devices report file.
        help="Path to write failed devices report.",
    )
    # Whether to return success even when some devices fail.
    parser.add_argument(
        # Ignore failures flag.
        "--ignore-failures",
        # Enable flag mode.
        action="store_true",
        # Help text for ignore failures behavior.
        help="Return exit code 0 even if some devices fail.",
    )
    # Return parsed arguments namespace.
    return parser.parse_args()


def load_device_info(device_file: Path) -> list[dict[str, object]]:
    # Ensure device config file exists.
    if not device_file.exists():
        # Fail fast with clear error.
        raise FileNotFoundError(f"Device file not found: {device_file}")

    # Open and read JSON device file.
    with device_file.open("r", encoding="utf-8") as file_obj:
        # Parse raw JSON into Python object.
        device_info = json.load(file_obj)

    # Support object format containing a "devices" list.
    if isinstance(device_info, dict) and "devices" in device_info:
        # Replace object with inner devices list.
        device_info = device_info["devices"]

    # Support single device object format.
    if isinstance(device_info, dict):
        # Normalize to a one-item list.
        return [device_info]

    # Support raw array of device objects.
    if isinstance(device_info, list) and all(isinstance(item, dict) for item in device_info):
        # Reject empty list.
        if not device_info:
            # Raise descriptive validation error.
            raise ValueError("Device file does not contain any devices.")
        # Return normalized list.
        return device_info

    # Reject unsupported JSON structure.
    raise ValueError(
        "Device file must contain a JSON object, a JSON array of device objects, or an object with a devices array."
    )


def validate_global_overrides(args: argparse.Namespace, device_count: int) -> None:
    # Disallow single-host override when running multiple devices.
    if device_count > 1 and args.host:
        # Raise conflict error.
        raise ValueError("--host cannot be used when device.json contains multiple devices.")
    # Disallow shared result file path when running multiple devices.
    if device_count > 1 and args.result_file:
        # Raise conflict error.
        raise ValueError("--result-file can only be used with a single device.")
    # Validate max-workers lower bound.
    if args.max_workers is not None and args.max_workers < 1:
        # Raise invalid value error.
        raise ValueError("--max-workers must be greater than 0.")
    # Enforce command timeout lower bound (>2 minutes).
    if args.command_timeout <= 120:
        # Raise invalid value error.
        raise ValueError("--command-timeout must be greater than 120 seconds.")
    # Validate per-device timeout lower bound.
    if args.session_timeout < 1:
        # Raise invalid value error.
        raise ValueError("--session-timeout must be greater than 0.")
    # Validate retry count lower bound.
    if args.retries < 1:
        # Raise invalid value error.
        raise ValueError("--retries must be greater than 0.")


def resolve_connection_info(args: argparse.Namespace, device_info: dict[str, object]) -> dict[str, object]:
    # Resolve host from CLI override or device record.
    host = args.host or device_info.get("host")
    # Resolve username from CLI override or device record.
    username = args.username or device_info.get("username")
    # Resolve password from CLI override, device record, or environment variable.
    password = args.password or device_info.get("password") or os.getenv("SSH_PASSWORD")
    # Resolve expected prompt from CLI override or device record.
    prompt = args.prompt or device_info.get("prompt")
    # Resolve port from CLI override or device record, default 22.
    port = args.port if args.port is not None else device_info.get("port", 22)

    # Validate host presence.
    if not host:
        # Raise if host missing.
        raise ValueError("Missing host. Set it in device.json or pass --host.")
    # Validate username presence.
    if not username:
        # Raise if username missing.
        raise ValueError("Missing username. Set it in device.json or pass --username.")
    # Validate password presence.
    if not password:
        # Raise if password missing.
        raise ValueError("Missing password. Set it in device.json, pass --password, or set SSH_PASSWORD.")

    # Convert port to integer safely.
    try:
        # Cast port to int.
        port = int(port)
    # Catch invalid type/value for port.
    except (TypeError, ValueError) as exc:
        # Raise clearer invalid port message.
        raise ValueError("Invalid port in device.json or --port.") from exc

    # Return normalized connection dictionary.
    return {
        # Normalized host.
        "host": host,
        # Normalized integer port.
        "port": port,
        # Normalized username.
        "username": username,
        # Normalized password.
        "password": password,
        # Optional expected prompt text.
        "prompt": prompt,
    }


def setup_logging(log_file: Path) -> logging.Logger:
    # Ensure parent log directory exists.
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Build/reuse named logger.
    logger = logging.getLogger("ssh_command_runner")
    # Set minimum log level.
    logger.setLevel(logging.INFO)
    # Remove existing handlers to avoid duplicates.
    logger.handlers.clear()

    # Shared log formatter for file and console.
    formatter = logging.Formatter(
        # Log output format.
        "%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    # Create file handler for persistent runtime logs.
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    # Apply formatter to file handler.
    file_handler.setFormatter(formatter)
    # Attach file handler to logger.
    logger.addHandler(file_handler)

    # Create stream handler for stdout.
    stream_handler = logging.StreamHandler(sys.stdout)
    # Apply formatter to stream handler.
    stream_handler.setFormatter(formatter)
    # Attach stream handler to logger.
    logger.addHandler(stream_handler)

    # Return configured logger.
    return logger


def load_commands(commands_file: Path) -> list[str]:
    # Ensure commands file exists.
    if not commands_file.exists():
        # Raise clear file-missing error.
        raise FileNotFoundError(f"Commands file not found: {commands_file}")

    # Initialize command list.
    commands: list[str] = []
    # Iterate each line in commands file.
    for raw_line in commands_file.read_text(encoding="utf-8").splitlines():
        # Trim whitespace around line.
        line = raw_line.strip()
        # Skip empty or comment lines.
        if not line or line.startswith("#"):
            # Continue to next line.
            continue
        # Append real command line.
        commands.append(line)

    # Ensure at least one executable command exists.
    if not commands:
        # Raise validation error for empty command set.
        raise ValueError(f"No executable commands found in {commands_file}")

    # Return parsed command list.
    return commands


def is_prompt_detected(output: str, expected_prompt: str | None) -> bool:
    # If explicit prompt text is configured, prefer exact containment check.
    if expected_prompt:
        # Return whether prompt text appears in output.
        return expected_prompt in output

    # Fallback prompt heuristic for common network-device prompts.
    return bool(re.search(r"(?m)^[^\r\n]*[>#]\s*$", output))


def read_channel_output(
    channel: paramiko.Channel,
    idle_wait: float,
    timeout: int,
    expected_prompt: str | None = None,
    enforce_min_timeout: bool = False,
) -> str:
    # Buffer chunks read from SSH channel.
    chunks: list[str] = []
    # Capture read start timestamp.
    start_time = time.time()
    # Track last time data was received.
    last_recv_time = time.time()
    # Track whether prompt has been observed.
    prompt_seen = False
    # Enforce >2 minutes timeout only for command reads, not for banner reads.
    effective_timeout = max(timeout, MIN_COMMAND_TIMEOUT_SECONDS) if enforce_min_timeout else timeout

    # Poll output until idle timeout or hard timeout.
    while True:
        # Check if channel has bytes ready.
        if channel.recv_ready():
            # Read available bytes and decode safely.
            data = channel.recv(65535).decode("utf-8", errors="ignore")
            # Save decoded chunk.
            chunks.append(data)
            # Update last receive timestamp.
            last_recv_time = time.time()
            # Update prompt detection state after receiving new output.
            if is_prompt_detected("".join(chunks), expected_prompt):
                # Mark prompt as seen.
                prompt_seen = True
            # Continue polling loop.
            continue

        # Get current time for timeout checks.
        now = time.time()
        # If channel closed unexpectedly, stop waiting immediately.
        if channel.closed:
            # Break polling loop.
            break
        # Stop only after prompt is seen and output has stayed idle for idle_wait.
        if prompt_seen and now - last_recv_time >= idle_wait:
            # Break polling loop.
            break
        # Stop when total wait exceeds hard timeout.
        if now - start_time >= effective_timeout:
            # Break polling loop.
            break
        # Sleep briefly to avoid busy loop.
        time.sleep(0.2)

    # Return concatenated raw output.
    return "".join(chunks)


def apply_terminal_controls(output: str) -> str:
    # Final rendered output lines.
    rendered_lines: list[str] = []
    # Current mutable line buffer.
    current_line: list[str] = []
    # Current cursor position in line.
    cursor = 0
    # Input scanning index.
    index = 0

    # Iterate through each character in raw output.
    while index < len(output):
        # Current character under scan.
        char = output[index]

        # Handle ESC-prefixed terminal control sequences.
        if char == "\x1b":
            # Try matching full ANSI CSI sequence.
            match = ANSI_ESCAPE_RE.match(output, index)
            # If ANSI sequence matched.
            if match:
                # Raw sequence text.
                sequence = match.group(0)
                # Final command letter.
                command = sequence[-1]
                # Parameter section without ESC[ and command byte.
                param_text = sequence[2:-1]
                # Parse numeric parameter, default 1.
                param = int(param_text) if param_text.isdigit() else 1

                # Move cursor left (e.g. ESC[1D).
                if command == "D":
                    # Clamp at start of line.
                    cursor = max(0, cursor - param)
                # Move cursor right (e.g. ESC[1C).
                elif command == "C":
                    # Advance cursor right.
                    cursor += param
                    # Fill gaps with spaces if cursor moved beyond current line length.
                    if cursor > len(current_line):
                        # Extend line with spaces.
                        current_line.extend(" " * (cursor - len(current_line)))
                # Erase from cursor to end of line (ESC[K).
                elif command == "K":
                    # Remove line tail from cursor onward.
                    del current_line[cursor:]

                # Move scanning index past matched escape sequence.
                index = match.end()
                # Continue to next scan iteration.
                continue

            # Try matching other short ESC forms.
            other_match = OTHER_ESCAPE_RE.match(output, index)
            # If short ESC sequence matched.
            if other_match:
                # Skip that sequence.
                index = other_match.end()
                # Continue scanning.
                continue

        # Handle newline by committing current line.
        if char == "\n":
            # Append trimmed current line to rendered output.
            rendered_lines.append("".join(current_line).rstrip())
            # Reset line buffer.
            current_line = []
            # Reset cursor for new line.
            cursor = 0
            # Move to next input character.
            index += 1
            # Continue scanning.
            continue

        # Handle carriage return by moving cursor to line start.
        if char == "\r":
            # Set cursor to column 0.
            cursor = 0
            # Move to next input character.
            index += 1
            # Continue scanning.
            continue

        # Handle backspace by moving cursor left.
        if char == "\b":
            # Move left but not below zero.
            cursor = max(0, cursor - 1)
            # Move to next input character.
            index += 1
            # Continue scanning.
            continue

        # Expand tab into spaces based on 8-column tab stop.
        if char == "\t":
            # Compute spaces required to next tab stop.
            spaces = 8 - (cursor % 8)
            # Write tab-expanded spaces.
            for _ in range(spaces):
                # If cursor at end, append a new space.
                if cursor >= len(current_line):
                    # Append space char.
                    current_line.append(" ")
                else:
                    # Overwrite existing position with space.
                    current_line[cursor] = " "
                # Advance cursor by one cell.
                cursor += 1
            # Move to next input character.
            index += 1
            # Continue scanning.
            continue

        # Only keep printable characters (space and above).
        if ord(char) >= 32:
            # If cursor is beyond current line end.
            if cursor >= len(current_line):
                # Pad gap with spaces.
                current_line.extend(" " * (cursor - len(current_line)))
                # Append new character.
                current_line.append(char)
            else:
                # Overwrite character at cursor position.
                current_line[cursor] = char
            # Advance cursor after write.
            cursor += 1

        # Advance input scan index by one.
        index += 1

    # Append final line after loop ends.
    rendered_lines.append("".join(current_line).rstrip())
    # Return fully rendered multi-line text.
    return "\n".join(rendered_lines)


def sanitize_output(output: str) -> str:
    # First replay terminal controls to remove visual artifacts.
    cleaned = apply_terminal_controls(output)
    # Remove any remaining ANSI CSI sequences.
    cleaned = ANSI_ESCAPE_RE.sub("", cleaned)
    # Remove any remaining short ESC sequences.
    cleaned = OTHER_ESCAPE_RE.sub("", cleaned)
    # Keep only printable chars plus newline and tab.
    cleaned = "".join(
        # Filter each character by allowed set.
        char for char in cleaned if char in "\n\t" or ord(char) >= 32
    )
    # Return sanitized output text.
    return cleaned


def build_result_file(host: str, configured_result_file: str | None) -> Path:
    # Use explicit result file when provided.
    if configured_result_file:
        # Resolve explicit path to absolute path.
        return Path(configured_result_file).expanduser().resolve()

    # Replace unsafe filename characters in host.
    safe_host = re.sub(r"[^A-Za-z0-9._-]", "_", host)
    # Build compact timestamp for filename.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Return auto-generated result file path.
    return BASE_DIR / f"{safe_host}_result_{timestamp}.log"


def write_result_header(result_file: Path, host: str, commands_file: Path) -> None:
    # Human-readable timestamp for file header.
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Build run metadata header block.
    header = (
        # Header separator line.
        f"\n{'=' * 80}\n"
        # Run time line.
        f"Run Time: {timestamp}\n"
        # Host line.
        f"Host: {host}\n"
        # Result file path line.
        f"Result File: {result_file}\n"
        # Commands file path line.
        f"Commands File: {commands_file}\n"
        # Footer separator line.
        f"{'=' * 80}\n"
    )
    # Ensure result file parent directory exists.
    result_file.parent.mkdir(parents=True, exist_ok=True)
    # Open result file in append mode.
    with result_file.open("a", encoding="utf-8") as file_obj:
        # Write header block to file.
        file_obj.write(header)


def append_command_result(
    result_file: Path,
    command: str,
    output: str,
    started_at: str | None = None,
    finished_at: str | None = None,
    elapsed_seconds: float | None = None,
) -> None:
    # Sanitize output before writing to file.
    cleaned_output = sanitize_output(output)
    # Build optional execution timing lines.
    timing_lines = ""
    if started_at is not None:
        timing_lines += f"[START] {started_at}\n"
    if finished_at is not None:
        timing_lines += f"[END] {finished_at}\n"
    if elapsed_seconds is not None:
        timing_lines += f"[DURATION] {elapsed_seconds:.3f}s\n"
    # Build per-command output block.
    block = (
        # Command title line.
        f"\n[COMMAND] {command}\n"
        # Optional command timing metadata.
        f"{timing_lines}"
        # Divider line.
        f"{'-' * 80}\n"
        # Command output or fallback marker.
        f"{cleaned_output.rstrip() or '[NO OUTPUT]'}\n"
        # Closing divider line.
        f"{'-' * 80}\n"
    )
    # Open result file in append mode.
    with result_file.open("a", encoding="utf-8") as file_obj:
        # Append block to result file.
        file_obj.write(block)


def connect_ssh(host: str, port: int, username: str, password: str | None, timeout: int) -> paramiko.SSHClient:
    # Create SSH client instance.
    client = paramiko.SSHClient()
    # Accept unknown host keys automatically.
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # Connect to remote SSH endpoint.
    client.connect(
        # Target hostname or IP.
        hostname=host,
        # Target SSH port.
        port=port,
        # Username for login.
        username=username,
        # Password for login.
        password=password,
        # Connect timeout in seconds.
        timeout=timeout,
        # SSH banner timeout.
        banner_timeout=timeout,
        # SSH authentication timeout.
        auth_timeout=timeout,
        # Disable local key lookup.
        look_for_keys=False,
        # Disable SSH agent usage.
        allow_agent=False,
    )
    # Return connected client.
    return client


def should_retry(exc: Exception) -> bool:
    # Authentication failures are usually not transient, do not retry.
    if isinstance(exc, paramiko.AuthenticationException):
        return False
    # These failures are commonly transient and can be retried.
    transient_errors = (
        paramiko.SSHException,
        TimeoutError,
        socket.timeout,
        OSError,
    )
    return isinstance(exc, transient_errors)


def classify_failure(exc: Exception) -> str:
    # Authentication issue category.
    if isinstance(exc, paramiko.AuthenticationException):
        return "authentication"
    # Timeout issue category.
    if isinstance(exc, (socket.timeout, TimeoutError)):
        return "timeout"
    # Connection issue category.
    if isinstance(exc, paramiko.ssh_exception.NoValidConnectionsError):
        return "connection"
    # Generic SSH protocol issue category.
    if isinstance(exc, paramiko.SSHException):
        return "ssh"
    # Generic OS/network issue category.
    if isinstance(exc, OSError):
        return "network"
    # Fallback unknown category.
    return "unknown"


def write_failed_devices_report(failed_file: Path, failed_records: list[dict[str, str]]) -> None:
    # Ensure parent directory exists.
    failed_file.parent.mkdir(parents=True, exist_ok=True)
    # Build report header timestamp.
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Build report lines.
    lines = [
        "\n" + "=" * 80,
        f"Run Time: {timestamp}",
        f"Failed Devices Count: {len(failed_records)}",
        "=" * 80,
    ]
    # Add one line per failed device.
    for record in failed_records:
        lines.append(
            f"Host={record['host']} | Reason={record['reason']} | Error={record['error']} | ResultFile={record['result_file']}"
        )
    # Write report in append mode.
    with failed_file.open("a", encoding="utf-8") as file_obj:
        file_obj.write("\n".join(lines) + "\n")


def run_commands(
    # Connected SSH client.
    client: paramiko.SSHClient,
    # List of CLI commands to execute.
    commands: list[str],
    # Result file path for this device.
    result_file: Path,
    # Shared logger instance.
    logger: logging.Logger,
    # Device host label for logging.
    host: str,
    # Optional prompt text for command completion detection.
    prompt: str | None,
    # Maximum total execution time per device.
    session_timeout: int,
    # Hard timeout per command output collection.
    command_timeout: int,
    # Idle interval used to detect end of output.
    command_interval: float,
) -> None:
    # Mark device execution start time.
    started_at = time.monotonic()

    # Helper to ensure one bad device cannot block forever.
    def ensure_session_time_budget() -> None:
        # Calculate elapsed time for this device.
        elapsed = time.monotonic() - started_at
        # If elapsed exceeds configured per-device limit, fail fast.
        if elapsed >= session_timeout:
            raise TimeoutError(f"Device session timeout reached: {session_timeout}s")

    # Check budget before opening shell.
    ensure_session_time_budget()
    # Open interactive shell channel.
    channel = client.invoke_shell(width=200, height=1000)
    # Wait a moment for login banner/prompt output.
    time.sleep(1)
    # Read initial banner text.
    banner = read_channel_output(
        channel,
        idle_wait=0.5,
        timeout=3,
        expected_prompt=None,
        enforce_min_timeout=False,
    )
    # If banner contains visible text.
    if banner.strip():
        # Persist banner in result file.
        append_command_result(result_file, "__SESSION_BANNER__", banner)

    # Iterate commands in order.
    for command in commands:
        # Ensure device execution has not exceeded session timeout.
        ensure_session_time_budget()
        # Compute remaining device-level budget.
        remaining_budget = max(1, int(session_timeout - (time.monotonic() - started_at)))
        # Track per-command timing.
        command_started_monotonic = time.monotonic()
        command_started_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Log command start for this host.
        logger.info("[%s] Executing command: %s", host, command)
        # Send command and trailing newline to shell.
        channel.send(command + "\n")
        # Read command output from channel.
        output = read_channel_output(
            # Current shell channel.
            channel,
            # Idle output settle interval.
            idle_wait=command_interval,
            # Hard command timeout.
            timeout=min(command_timeout, remaining_budget),
            # Prompt text to determine command completion.
            expected_prompt=prompt,
            # For command output, enforce >2 minute floor.
            enforce_min_timeout=True,
        )
        # Finalize per-command timing.
        command_finished_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        command_elapsed_seconds = time.monotonic() - command_started_monotonic
        # Append command output block to result file.
        append_command_result(
            result_file,
            command,
            output,
            started_at=command_started_text,
            finished_at=command_finished_text,
            elapsed_seconds=command_elapsed_seconds,
        )
        # Log command completion for this host.
        logger.info("[%s] Finished command: %s", host, command)

    # Close shell channel after all commands complete.
    channel.close()


def execute_device(
    # Parsed CLI args.
    args: argparse.Namespace,
    # Preloaded command list.
    commands: list[str],
    # Commands file path for header metadata.
    commands_file: Path,
    # Shared logger instance.
    logger: logging.Logger,
    # Single device dictionary.
    device_info: dict[str, object],
) -> tuple[str, bool, Path | None, str | None, str | None]:
    # Resolve effective connection fields for this device.
    connection_info = resolve_connection_info(args, device_info)
    # Get host as string for logging and filenames.
    host = str(connection_info["host"])
    # Build per-run result file path for this host.
    result_file = build_result_file(host, args.result_file)

    # Retry per device for transient failures.
    for attempt in range(1, args.retries + 1):
        # Wrap all per-device operations to isolate failures.
        try:
            # Write file header once before first attempt.
            if attempt == 1:
                write_result_header(result_file, host, commands_file)
            # Log SSH session start with attempt index.
            logger.info("[%s] Starting SSH session to %s:%s (attempt %s/%s)", host, host, connection_info["port"], attempt, args.retries)
            # Log chosen result file path.
            logger.info("[%s] Result file: %s", host, result_file)

            # Establish SSH connection.
            client = connect_ssh(
                # Host for this connection.
                host=host,
                # Port for this connection.
                port=int(connection_info["port"]),
                # Username for this connection.
                username=str(connection_info["username"]),
                # Password for this connection.
                password=str(connection_info["password"]),
                # Connect timeout from CLI args.
                timeout=args.connect_timeout,
            )
            # Ensure client closes even if command execution fails.
            try:
                # Execute all commands on connected device.
                run_commands(
                    # Active SSH client.
                    client=client,
                    # Command list.
                    commands=commands,
                    # Result file path.
                    result_file=result_file,
                    # Shared logger.
                    logger=logger,
                    # Host label.
                    host=host,
                    # Prompt text.
                    prompt=str(connection_info["prompt"]) if connection_info["prompt"] is not None else None,
                    # Total timeout for this device run.
                    session_timeout=args.session_timeout,
                    # Per-command timeout.
                    command_timeout=args.command_timeout,
                    # Output idle interval.
                    command_interval=args.command_interval,
                )
            finally:
                # Always close SSH client.
                client.close()

            # Log successful completion for this host.
            logger.info("[%s] Execution completed successfully", host)
            # Return success tuple.
            return host, True, result_file, None, None
        # Catch all exceptions for this device attempt.
        except Exception as exc:
            # Normalize common network timeout to readable error type.
            if isinstance(exc, (socket.timeout, TimeoutError)):
                logger.error("[%s] Timeout: %s", host, exc)
            # Decide whether this failure type should be retried.
            retryable = should_retry(exc)
            # If this is the last attempt or non-retryable failure, exit.
            if (attempt >= args.retries) or (not retryable):
                # Classify failure reason for summary reporting.
                failure_reason = classify_failure(exc)
                # Log full exception with traceback.
                logger.exception("[%s] Execution failed: %s", host, exc)
                # Return failure tuple.
                return host, False, result_file, failure_reason, str(exc)
            # Log retry action for this device.
            logger.warning("[%s] Attempt %s/%s failed: %s; retrying...", host, attempt, args.retries, exc)
            # Small backoff before retry.
            time.sleep(1)

    # Should never be reached, keep safe fallback.
    return host, False, result_file, "unknown", "Unexpected device execution flow"


def run_parallel(
    # Parsed CLI args.
    args: argparse.Namespace,
    # Shared command list.
    commands: list[str],
    # Commands file path.
    commands_file: Path,
    # Shared logger.
    logger: logging.Logger,
    # List of devices to execute.
    device_list: list[dict[str, object]],
) -> int:
    # Determine effective worker count.
    max_workers = args.max_workers or len(device_list)
    # Count failed device executions.
    failure_count = 0
    # Collect failed device details for reporting.
    failed_records: list[dict[str, str]] = []

    # Create thread pool for concurrent device execution.
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit one future per device.
        futures = [
            # Schedule device execution task.
            executor.submit(execute_device, args, commands, commands_file, logger, device_info)
            # Iterate all device records.
            for device_info in device_list
        ]

        # Process futures as they complete.
        for future in concurrent.futures.as_completed(futures):
            # Unpack per-device result tuple.
            host, success, result_file, failure_reason, error_message = future.result()
            # Handle successful device run.
            if success:
                # Log success and output file path.
                logger.info("[%s] Finished with result file %s", host, result_file)
            else:
                # Increment failure counter.
                failure_count += 1
                # Store failure details for report output.
                failed_records.append(
                    {
                        "host": host,
                        "reason": failure_reason or "unknown",
                        "error": error_message or "Unknown error",
                        "result_file": str(result_file) if result_file else "N/A",
                    }
                )

    # If any device failed.
    if failure_count:
        # Write failed devices report file.
        failed_file = Path(args.failed_devices_file).expanduser().resolve()
        write_failed_devices_report(failed_file, failed_records)
        # Summarize failure reasons.
        reason_counter = Counter(record["reason"] for record in failed_records)
        # Log failure reason summary.
        logger.error("Failure reason summary: %s", dict(reason_counter))
        # Log failed devices report location.
        logger.error("Failed devices report written to: %s", failed_file)
        # Log final failure summary.
        logger.error("Completed with %s failed device(s)", failure_count)
        # Return success when ignore-failures is enabled.
        if args.ignore_failures:
            logger.warning("ignore-failures enabled, returning success exit code despite failures")
            return 0
        # Return non-zero exit code by default.
        return 1

    # Log all-success summary.
    logger.info("All device executions completed successfully")
    # Return success exit code.
    return 0


def main() -> int:
    # Parse CLI arguments.
    args = parse_args()

    # Resolve device file path.
    device_file = Path(args.device_file).expanduser().resolve()
    # Resolve commands file path.
    commands_file = Path(args.commands_file).expanduser().resolve()
    # Resolve runtime log file path.
    log_file = Path(args.log_file).expanduser().resolve()

    # Initialize shared logger.
    logger = setup_logging(log_file)

    # Wrap top-level workflow for uniform error handling.
    try:
        # Load and normalize device list.
        device_list = load_device_info(device_file)
        # Validate global overrides against device count.
        validate_global_overrides(args, len(device_list))
        # Load executable commands.
        commands = load_commands(commands_file)
        # Log number of loaded devices.
        logger.info("Loaded %s device(s) from %s", len(device_list), device_file)
        # Run multi-device execution and return exit code.
        return run_parallel(args, commands, commands_file, logger, device_list)
    # Catch and log any top-level error.
    except Exception as exc:
        # Log full traceback for diagnostics.
        logger.exception("Execution failed: %s", exc)
        # Return error exit code.
        return 1


def running_under_debugger() -> bool:
    # Generic runtime debugger hook used by debuggers and tracers.
    if sys.gettrace() is not None:
        return True
    # VS Code / debugpy launcher marker.
    if os.getenv("DEBUGPY_LAUNCHER_PORT"):
        return True
    # PyDev-compatible debugger markers.
    if os.getenv("PYDEVD_USE_FRAME_EVAL") or os.getenv("PYDEVD_LOAD_VALUES_ASYNC"):
        return True
    return False


# Run main only when executed as a script.
if __name__ == "__main__":
    # Capture script exit code.
    exit_code = main()
    # In debugger sessions, avoid raising SystemExit which is often treated as an exception stop.
    if running_under_debugger():
        # Print final status and return control to debugger.
        if exit_code != 0:
            print(f"Script finished with exit code {exit_code}. Check log/run.log for details.")
    else:
        # Keep standard CLI exit behavior outside debugger.
        raise SystemExit(exit_code)
