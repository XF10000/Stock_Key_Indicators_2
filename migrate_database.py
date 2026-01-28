#!/usr/bin/env python3
"""
数据库迁移脚本

功能：
1. 备份现有数据库
2. 创建新的数据库表结构（支持287个字段）
3. 可选：从旧数据库迁移数据到新数据库

使用方法：
    python migrate_database.py [--backup-only] [--migrate-data]
    
参数：
    --backup-only: 仅备份数据库，不创建新表
    --migrate-data: 迁移旧数据到新数据库（需要手动实现映射逻辑）
"""

import argparse
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

import models


def backup_database(db_path: str = "database.sqlite") -> str:
    """备份现有数据库
    
    Args:
        db_path: 数据库文件路径
        
    Returns:
        备份文件路径
    """
    if not Path(db_path).exists():
        print(f"数据库文件不存在: {db_path}")
        return ""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{db_path}.backup_{timestamp}"
    
    try:
        shutil.copy2(db_path, backup_path)
        file_size = Path(backup_path).stat().st_size / (1024 * 1024)  # MB
        print(f"✓ 数据库备份成功: {backup_path} ({file_size:.2f} MB)")
        return backup_path
    except Exception as e:
        print(f"✗ 数据库备份失败: {e}")
        return ""


def create_new_database(db_path: str = "database_new.sqlite") -> bool:
    """创建新的数据库表结构
    
    Args:
        db_path: 新数据库文件路径
        
    Returns:
        是否成功
    """
    try:
        # 如果文件已存在，先删除
        if Path(db_path).exists():
            Path(db_path).unlink()
            print(f"✓ 删除旧的数据库文件: {db_path}")
        
        # 创建新表
        models.create_tables(f"sqlite:///{db_path}")
        
        # 验证表结构
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print(f"\n✓ 新数据库创建成功: {db_path}")
        print(f"  包含 {len(tables)} 个表:")
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table[0]})")
            columns = cursor.fetchall()
            print(f"    - {table[0]}: {len(columns)} 个字段")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ 创建新数据库失败: {e}")
        return False


def migrate_data(old_db: str = "database.sqlite", new_db: str = "database_new.sqlite") -> bool:
    """从旧数据库迁移数据到新数据库
    
    注意：此功能需要根据实际情况实现字段映射逻辑
    
    Args:
        old_db: 旧数据库路径
        new_db: 新数据库路径
        
    Returns:
        是否成功
    """
    print("\n数据迁移功能暂未实现")
    print("建议：重新运行数据采集程序以填充新数据库")
    return False


def main():
    parser = argparse.ArgumentParser(description="数据库迁移脚本")
    parser.add_argument("--backup-only", action="store_true", help="仅备份数据库")
    parser.add_argument("--migrate-data", action="store_true", help="迁移旧数据到新数据库")
    parser.add_argument("--old-db", default="database.sqlite", help="旧数据库路径")
    parser.add_argument("--new-db", default="database_new.sqlite", help="新数据库路径")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("数据库迁移脚本")
    print("=" * 80)
    
    # 1. 备份现有数据库
    print("\n步骤 1: 备份现有数据库")
    print("-" * 80)
    backup_path = backup_database(args.old_db)
    
    if args.backup_only:
        print("\n仅备份模式，退出")
        return
    
    # 2. 创建新数据库
    print("\n步骤 2: 创建新数据库表结构")
    print("-" * 80)
    success = create_new_database(args.new_db)
    
    if not success:
        print("\n✗ 创建新数据库失败，退出")
        return
    
    # 3. 迁移数据（可选）
    if args.migrate_data:
        print("\n步骤 3: 迁移数据")
        print("-" * 80)
        migrate_data(args.old_db, args.new_db)
    
    print("\n" + "=" * 80)
    print("迁移完成！")
    print("=" * 80)
    print(f"\n旧数据库备份: {backup_path}")
    print(f"新数据库: {args.new_db}")
    print("\n下一步:")
    print("1. 检查新数据库表结构")
    print("2. 运行数据采集程序填充新数据库")
    print("3. 验证数据完整性")
    print("4. 将新数据库重命名为 database.sqlite")


if __name__ == "__main__":
    main()
