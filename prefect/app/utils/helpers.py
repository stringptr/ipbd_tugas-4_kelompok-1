"""Shared utility functions for Prefect flows."""

import json
from pathlib import Path
from typing import Any
from datetime import datetime
import logging

from prefect import get_run_logger


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f"prefect_{datetime.now().strftime('%Y%m%d')}.log"),
        ],
    )


def save_checkpoint(
    data: Any, checkpoint_name: str, checkpoint_dir: str = "./checkpoints"
) -> Path:
    """
    Save a checkpoint of data for debugging or recovery.

    Args:
        data: Data to save (will be converted to JSON if dict/list)
        checkpoint_name: Name of the checkpoint
        checkpoint_dir: Directory to save checkpoints

    Returns:
        Path to saved checkpoint
    """
    checkpoint_path = Path(checkpoint_dir)
    checkpoint_path.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = checkpoint_path / f"{checkpoint_name}_{timestamp}.json"

    with open(filename, "w") as f:
        if isinstance(data, (dict, list)):
            json.dump(data, f, indent=2, default=str)
        else:
            f.write(str(data))

    logger = get_run_logger()
    logger.info(f"Checkpoint saved: {filename}")

    return filename


def load_checkpoint(checkpoint_path: str) -> dict[str, Any]:
    """
    Load a checkpoint file.

    Args:
        checkpoint_path: Path to checkpoint file

    Returns:
        Loaded data
    """
    with open(checkpoint_path, "r") as f:
        if checkpoint_path.endswith(".json"):
            return json.load(f)
        else:
            return {"data": f.read()}


def log_metrics(metrics: dict[str, Any], prefix: str = "") -> None:
    """
    Log metrics for monitoring.

    Args:
        metrics: Dictionary of metrics
        prefix: Optional prefix for metric names
    """
    logger = get_run_logger()

    for key, value in metrics.items():
        metric_name = f"{prefix}.{key}" if prefix else key
        logger.info(f"Metric - {metric_name}: {value}")
