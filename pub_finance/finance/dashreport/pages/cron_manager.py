import re
import subprocess


class CronManager:
    SCRIPT_MAPPING = {
        "cnstock_main.py": "cn_stock_cron",
        "usstock_main.py": "us_stock_cron",
        "fetch_cn_proxies.py": "cn_proxy_cron",
        "fetch_overseas_proxies.py": "oversea_proxy_cron",
    }

    @classmethod
    def update_system_cron(cls, schedule_cfg: dict) -> bool:
        try:
            # 1. 读取系统当前 crontab
            res = subprocess.run(
                ["crontab", "-l"], capture_output=True, text=True, check=True
            )
            raw_crontab = res.stdout
        except subprocess.CalledProcessError:
            return False

        if not raw_crontab.strip():
            return False

        lines = raw_crontab.splitlines()
        updated_lines = []

        # 2. 精准匹配脚本名字，并替换最左侧 5 段时间表达式
        for line in lines:
            line_str = line.strip()
            if not line_str or line_str.startswith("#"):
                updated_lines.append(line)
                continue

            for script_name, config_key in cls.SCRIPT_MAPPING.items():
                if script_name in line:
                    new_cron = schedule_cfg.get(config_key, "").strip()
                    if new_cron:
                        pattern = r"^\s*([^\s]+\s+){5}"
                        line = re.sub(pattern, f"{new_cron} ", line, count=1)
                    break

            updated_lines.append(line)

        new_crontab_str = "\n".join(updated_lines) + "\n"

        # 3. 写回系统 crontab
        try:
            subprocess.run(
                ["crontab", "-"],
                input=new_crontab_str,
                text=True,
                capture_output=True,
                check=True,
            )
            return True
        except subprocess.CalledProcessError:
            return False
