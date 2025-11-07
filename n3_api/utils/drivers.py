# noema/n3_api/utils/drivers.py
from typing import Any, Dict

def build_drivers_safe() -> Dict[str, Any]:
    """
    یک سازندهٔ امن که همیشه شکل استاندارد برمی‌گرداند:
      drivers["skills"]["execute"] : callable
      drivers["transport"]["emit"] : callable
      drivers["transport"]["outbox"]: callable
    و آلیاس‌های تخت: transport_emit / transport_outbox
    """
    drivers: Dict[str, Any] = {}

    # 1) اگر builder خارجی بود، استفاده کن
    try:
        from examples.minimal_chat.drivers_dev import build_drivers as ext_build
        d = ext_build() or {}
        if isinstance(d, dict):
            drivers.update(d)
    except Exception:
        pass

    # 2) skills را نرمال کن
    skills = drivers.get("skills")
    if isinstance(skills, dict) and callable(skills.get("execute")):
        pass
    elif callable(skills):
        drivers["skills"] = {"execute": skills}
    else:
        from n3_drivers.skills import local_runner
        drivers["skills"] = {"execute": local_runner.execute}

    # 3) transport را نرمال کن
    t_emit = t_out = None
    t = drivers.get("transport")
    if isinstance(t, dict):
        t_emit = t.get("emit")
        t_out  = t.get("outbox")

    if not (callable(t_emit) and callable(t_out)):
        try:
            from n3_drivers.transport import http_dev
            t_emit = getattr(http_dev, "emit", t_emit)
            t_out  = getattr(http_dev, "outbox", t_out)
        except Exception:
            pass

    # fallback درون‌حافظه‌ای
    if not callable(t_emit) or not callable(t_out):
        _BUF = []
        def _emit(item):
            _BUF.append(item); return True
        def _outbox():
            return list(_BUF)
        t_emit, t_out = _emit, _outbox

    drivers["transport"] = {"emit": t_emit, "outbox": t_out}
    # آلیاس‌های تخت برای راحتی برخی مسیرها
    drivers["transport_emit"] = t_emit
    drivers["transport_outbox"] = t_out
    return drivers
