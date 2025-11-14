import csv
import os
import re
from collections import defaultdict

import django


def setup_django():
    """
    初始化 Django，讓這支腳本可以使用 ORM。
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if base_dir not in os.sys.path:
        os.sys.path.append(base_dir)

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djangoProject.settings")
    django.setup()


def extract_annotator_id(email: str) -> str:
    """
    從 email 抽出 + 後面的數字，例如:
      lobsterlabannotator+4@gmail.com -> '4'
    如果沒有 +number，就回傳原始 email（當成 ID 使用）。
    """
    if not email:
        return ""

    m = re.search(r"\+(\d+)", email)
    if m:
        return m.group(1)

    # 特例：這個 email 視為 annotator=12
    if email == "lobsterlabcsnthu@gmail.com":
        return "12"

    return email


def build_dataset(input_path: str) -> int:
    """
    從 data.csv 讀資料，依 task_id 聚合後寫入 SQLite（main.Dataset）：
      - task_id: CSV 中的 id 欄位
      - img: image_url
      - query: query
      - item: IT_NO
      - it_name: IT_NAME
      - annotator: 以逗號串起來的 annotator id（例如 "1,4,7,11"）

    同一個 task_id 只存一筆，annotator 會合併並覆蓋舊資料。
    回傳寫入/更新的筆數。
    """
    from main.models import Dataset

    grouped = {}

    with open(input_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            task_id = row.get("id")
            if not task_id:
                continue

            it_name = row.get("IT_NAME", "")
            it_no = row.get("IT_NO", "")
            img = row.get("image_url", "")
            query = row.get("query", "")
            annotator_email = row.get("annotator", "")

            annotator_id = extract_annotator_id(annotator_email)

            if task_id not in grouped:
                grouped[task_id] = {
                    "task_id": task_id,
                    "img": img,
                    "query": query,
                    "item": it_no,
                    "it_name": it_name,
                    "annotators": set(),
                }

            if annotator_id:
                grouped[task_id]["annotators"].add(annotator_id)

    affected = 0

    for task_id, data in grouped.items():
        annotators = sorted(
            data["annotators"],
            key=lambda x: (not x.isdigit(), int(x) if x.isdigit() else x),
        )
        annotator_str = ",".join(annotators)

        _, _created = Dataset.objects.update_or_create(
            task_id=int(data["task_id"]),
            defaults={
                "img": data["img"],
                "query": data["query"],
                "item": data["item"],
                "it_name": data["it_name"],
                "annotator": annotator_str,
            },
        )
        affected += 1

    return affected


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_csv = os.path.join(base_dir, "data.csv")

    if not os.path.exists(input_csv):
        raise SystemExit(f"找不到輸入檔案: {input_csv}")

    setup_django()
    count = build_dataset(input_csv)
    print(f"已完成 dataset 寫入 SQLite，處理 {count} 筆資料")

