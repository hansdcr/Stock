import yaml
import os
import sys


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, THIS_DIR)

config_file = os.path.join(os.path.dirname(THIS_DIR), "config", "config.yaml")
if not os.path.exists(config_file):
    print(f"[ERROR] 配置文件不存在: {config_file}")
    raise FileNotFoundError(f"[ERROR] 配置文件不存在: {config_file}")


class ParseConfig:
    def __init__(self):
        self.config_file = config_file
        with open(self.config_file, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)
        if self.config is None:
            raise ValueError(f"Failed to load config file {self.config_file}")
    
    def get_token(self):
        return self.config.get("token")
    
    def get_mysql_config(self):
        return self.config.get("mysql", {})

if __name__ == "__main__":
    cfg = ParseConfig()
    token = cfg.get_token()
    print(token)
    mysql_config = cfg.get_mysql_config()
    print(mysql_config.get("host"))
    print(mysql_config.get("port"))
    print(mysql_config.get("user"))
    print(mysql_config.get("password"))
    print(mysql_config.get("database"))
    