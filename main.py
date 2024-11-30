import os
import time
import subprocess
from datetime import datetime
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler


class PostgresBackup:
    def __init__(
        self,
        container_name: str,
        postgres_user: str,
        backup_dir: str,
        databases: list[str],
        retention_days: int,
    ):
        self.container_name = container_name
        self.postgres_user = postgres_user
        self.backup_dir = backup_dir
        self.databases = databases
        self.retention_days = retention_days
        # 确保备份目录存在
        os.makedirs(self.backup_dir, exist_ok=True)

        # 设置带有自动轮转的日志
        log_file = os.path.join(self.backup_dir, "backup.log")
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        handler = RotatingFileHandler(
            log_file,
            maxBytes=1024 * 1024,  # 1MB
            backupCount=7,  # 保留7个备份文件
            encoding="utf-8",
        )
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )

        logger = logging.getLogger("db_backup")
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        self.logger = logger

    def run_cmd(self, cmd, dry_run=False):
        """统一的命令执行函数"""
        cmd_str = " ".join(cmd)
        if dry_run:
            self.logger.info(f"[DRY RUN] {cmd_str}")
            return

        self.logger.info(f"执行命令: {cmd_str}")
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            return result
        except subprocess.CalledProcessError as e:
            self.logger.error(f"命令执行失败: {e.stderr}")
            raise

    def backup_database(self, db_name, dry_run=False):
        """备份单个数据库"""
        try:
            # 准备文件路径
            date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"{db_name}_{date_str}.dump"
            container_path = f"/tmp/{backup_filename}"
            local_path = os.path.join(self.backup_dir, backup_filename)

            # 执行备份命令
            commands = [
                # pg_dump 命令
                [
                    "docker",
                    "exec",
                    self.container_name,
                    "pg_dump",
                    "-U",
                    self.postgres_user,
                    "-d",
                    db_name,
                    "-F",
                    "c",
                    "-f",
                    container_path,
                ],
                # 复制文件到本地
                ["docker", "cp", f"{self.container_name}:{container_path}", local_path],
                # 清理临时文件
                ["docker", "exec", self.container_name, "rm", container_path],
            ]

            # 执行所有命令
            for cmd in commands:
                self.run_cmd(cmd, dry_run)

            self.logger.info(f"{'[DRY RUN] ' if dry_run else ''}数据库 {db_name} 备份完成")

        except Exception as e:
            self.logger.error(f"备份失败 {db_name}: {str(e)}")

    def cleanup_old_backups(self, dry_run=False):
        """清理旧备份"""
        try:
            # 计算过期时间
            max_age = self.retention_days * 24 * 60 * 60
            current_time = time.time()

            # 检查并删除旧文件
            for file in Path(self.backup_dir).glob("*.dump"):
                if current_time - file.stat().st_mtime > max_age:
                    if dry_run:
                        self.logger.info(f"[DRY RUN] 将删除: {file}")
                    else:
                        file.unlink()
                        self.logger.info(f"已删除: {file}")

        except Exception as e:
            self.logger.error(f"清理失败: {str(e)}")

    def run(self, dry_run=False):
        """执行完整备份流程"""
        self.logger.info(f"{'[DRY RUN] ' if dry_run else ''}开始备份...")
        try:
            # 创建备份目录
            os.makedirs(self.backup_dir, exist_ok=True)

            # 备份所有数据库
            for db in self.databases:
                self.backup_database(db, dry_run)

            # 清理旧备份
            self.cleanup_old_backups(dry_run)

            self.logger.info(f"{'[DRY RUN] ' if dry_run else ''}备份完成")
            return True
        except Exception as e:
            self.logger.error(f"备份过程失败: {str(e)}")
            return False


if __name__ == "__main__":
    # dev环境备份
    dev_backup = PostgresBackup(
        container_name="postgis",
        postgres_user="postgres",
        backup_dir="/opt/hdd/postgreSQLBackup/dev",
        databases=["USD_sample", "postgres"],
        retention_days=365
    )
    dev_success = dev_backup.run(dry_run=False)

    # test环境备份
    test_backup = PostgresBackup(
        container_name="testpostgis",
        postgres_user="postgres",
        backup_dir="/opt/hdd/postgreSQLBackup/test",
        databases=["postgres"],
        retention_days=365
    )
    test_success = test_backup.run(dry_run=False)
    exit(0 if (dev_success and test_success) else 1)