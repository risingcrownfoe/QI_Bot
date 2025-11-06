from copy import deepcopy
from pathlib import Path
from typing import Dict, Any, List

def resolve_event(event: Dict[str, Any], schedule_data: Dict[str, Any]) -> Dict[str, Any]:
    """Apply template merge, vars formatting, and image normalization."""
    out = deepcopy(event)

    tmpl_name = out.pop("use", None)
    if tmpl_name:
        tmpl = schedule_data.get("templates", {}).get(tmpl_name)
        if tmpl:
            merged = deepcopy(tmpl)
            merged.update(out)
            out = merged

    # remove explicit null image
    if "image" in out and out["image"] is None:
        out.pop("image", None)

    # format variables into text
    vars_dict = out.pop("vars", None)
    if isinstance(vars_dict, dict) and isinstance(out.get("text"), str):
        try:
            out["text"] = out["text"].format(**vars_dict)
        except Exception:
            # keep text unformatted on error
            pass

    return out

def collect_files(image_field: Any) -> List[Path]:
    """Normalize image field to a list of existing Paths."""
    files: List[Path] = []
    if not image_field:
        return files
    paths = image_field if isinstance(image_field, list) else [image_field]
    for p in paths:
        fp = Path(p)
        if fp.exists():
            files.append(fp)
    return files
