from django.utils import timezone as dj_tz
from datetime import timezone as dt_tz
from django.http import JsonResponse, HttpResponseBadRequest
import requests
from django.conf import settings
from django.shortcuts import render
from django.http import HttpResponseServerError
from datetime import datetime, timezone
from django.views.decorators.csrf import csrf_exempt
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import re
from collections import defaultdict
import json
from django.conf import settings
import os
from .models import Task, Annotation


INPUT_CSV = os.path.join(settings.BASE_DIR, "data.csv")
OUTPUT_CSV = os.path.join(settings.BASE_DIR, "output.csv")

MAX_WORKERS = 8
LS_URL = getattr(settings, "LABEL_STUDIO_URL")            # 例如: "https://app.humansignal.com"
LS_TOKEN = getattr(settings, "LABEL_STUDIO_TOKEN")        # 這是你的 PAT（Personal Access Token）
PROJECT_ID = int(getattr(settings, "PROJECT_ID"))
MY_UID = int(getattr(settings, "MY_UID"))
total = int(getattr(settings, "TOTAL"))
ALLOWED_REL = {'E', 'S', 'C', 'I'}  # ESCI
FETCH_NUM = 100
task_ids = []

def get_access_token():
    refresh_url = f"{LS_URL}/api/token/refresh/"
    r = requests.post(refresh_url, json={"refresh": LS_TOKEN}, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data["access"]

def make_headers(access_token: str):
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

def post_annotation(access, task_id, rating="0", relation="I"):

    try:
        task_id = int(task_id)
    except (TypeError, ValueError):
        return False, f"task_id 非整數：{task_id!r}"
    if task_id <= 0:
        return False, f"task_id 不可為 0 或負數：{task_id}"

    # rating / relation 正規化
    rating = str(rating).strip()
    relation = str(relation).strip().upper()

    allowed_ratings = {"0", "1", "2", "3", "4"}
    allowed_rel = {"E", "S", "C", "I"}
    # 支援全名（Exact/Substitute/Complement/Irrelevant）
    full2abbr = {"EXACT": "E", "SUBSTITUTE": "S", "COMPLEMENT": "C", "IRRELEVANT": "I"}
    if relation not in allowed_rel:
        relation = full2abbr.get(relation.upper(), relation)
    if rating not in allowed_ratings:
        return False, f"rating 僅允許 {sorted(allowed_ratings)}，收到：{rating}"
    if relation not in allowed_rel:
        return False, f"relation 僅允許 {sorted(allowed_rel)}，收到：{relation}"

    headers = make_headers(access)

    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # ---- payload：不要放 project / task ----
    payload = {
        "lead_time": 5.0,
        "started_at": now_iso,
        "result": [
            {
                "from_name": "rating",
                "to_name": "query",
                "type": "choices",
                "origin": "manual",
                "value": {"choices": [rating]},
            },
            {
                "from_name": "relation",
                "to_name": "query",
                "type": "choices",
                "origin": "manual",
                "value": {"choices": [relation]},
            },
        ],
    }


    url = f"{LS_URL}/api/tasks/{task_id}/annotations/"
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        if r.status_code not in (200, 201):
            return False, f"annotation 失敗 {r.status_code} {r.text}"
        return True, r.json()
    except requests.RequestException as e:
        return False, f"HTTP 錯誤：{e}"



def extract_annotator_id(email: str) -> str:
    m = re.search(r"\+(\d+)", email)
    if m:
        return m.group(1)
    return email  # fallback

def get_data(annotator=None):
    # key: (IT_NAME, image_url, query)
    # value: { "id": id, "annotators": set([...]) }
    grouped = defaultdict(lambda: {"id": None, "annotators": set()})

    with open(INPUT_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            it_name = row["IT_NAME"]
            img = row["image_url"]
            query = row["query"]
            annotator_email = row["annotator"]
            
            sample_id = row["id"]    

            annotator_id = extract_annotator_id(annotator_email)
            if annotator_id == 'lobsterlabcsnthu@gmail.com':
                annotator_id = '12'
            key = (it_name, img, query)

            # 保存 id（只存第一次，後面都是同一個 id）
            if grouped[key]["id"] is None:
                grouped[key]["id"] = sample_id

            grouped[key]["annotators"].add(annotator_id)

    # 最終輸出給前端用
    result = []

    # 同時寫出 output.csv
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        fieldnames = ["id", "IT_NAME", "image_url", "query", "annotators"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for (it_name, img, query), data in grouped.items():

            annotator_ids = data["annotators"]


            # 若有指定 annotator，就只保留包含此 annotator 的樣本
            if annotator is not None and str(annotator) not in annotator_ids:
                continue

            ids_sorted = sorted(
                annotator_ids,
                key=lambda x: (not x.isdigit(), int(x) if x.isdigit() else x)
            )
            annotators_list = ids_sorted  # 例如 ['1','4','7','11']
            row_dict = {
                "id": data["id"],
                "IT_NAME": it_name,
                "image_url": img,
                "query": query,
                "annotators": annotators_list,   
            }
            writer.writerow(row_dict)
            result.append(row_dict)

    return result


@csrf_exempt
def index(request):
    if request.method == 'GET':
        # ?annotator=1~12，若沒給或是 all 則不篩選
        annotator = request.GET.get("annotator")
        if annotator in ("", None, "all"):
            annotator = None

        rows = get_data(annotator=annotator)

        # 依照每列的 id (task_id) 把已存在的 Annotation 資訊補回去
        for row in rows:
            task_id_str = row.get("id")
            if not task_id_str:
                continue

            # 利用 id 去查找 Task
            try:
                task_id_int = int(task_id_str)
            except (TypeError, ValueError):
                continue

            task = Task.objects.filter(task_id=task_id_int).first()
            if not task:
                # 這個 task 目前沒有任何 Annotation
                continue

            # 用 task 去尋找外鍵指向他的 Annotation，建立 {annotator: (rating, relation)} map
            ann_map = {
                str(a.annotation_id): (a.rating, a.relation)
                for a in task.annotations.all()
            }
            if not ann_map:
                continue

            # 將 'annotators': ['1','4','7'] 改成
            # 每個元素都變成 dict:
            # 有資料: {'annotator': '1', 'rating': 4, 'relation': 'E'}
            # 沒有資料: {'annotator': '1', 'rating': None, 'relation': None}
            original_ann_list = row.get("annotators", [])
            new_ann_list = []
            for ann in original_ann_list:
                ann_str = str(ann)
                rating, relation = ann_map.get(ann_str, (None, None))
                new_ann_list.append(
                    {
                        "annotator": ann_str,
                        "rating": rating,
                        "relation": relation,
                    }
                )
            row["annotators"] = new_ann_list
        return render(
            request,
            "tables.html",
            {
                "rows": rows,
                "annotator": annotator,
                "next_task_id": 223471874,
            },
        )

    if request.method == 'POST':
        try:
            payload = json.loads(request.body.decode("utf-8"))
        except json.JSONDecodeError:
            return HttpResponseBadRequest("Invalid JSON")

        items = payload.get("items")
        if not isinstance(items, list):
            return HttpResponseBadRequest("Invalid payload: items must be a list")

        created_or_updated = 0

        for item in items:
            task_id_raw = item.get("task_id")
            annotator_raw = item.get("annotator")
            value_raw = str(item.get("value", "")).strip()

            # 必須有 task_id 與輸入值，沒寫的不處理
            if not task_id_raw or not value_raw:
                continue

            # 從 value 裡解析 rating(0-4) 與 relation(E/S/C/I)
            m_rating = re.search(r"[0-4]", value_raw)
            m_rel = re.search(r"[ESCI]", value_raw, re.IGNORECASE)
            if not m_rating or not m_rel:
                # 格式不對就略過，不中斷整批
                continue

            try:
                rating = int(m_rating.group(0))
            except ValueError:
                continue
            relation = m_rel.group(0).upper()

            # 建立或取得 Task
            try:
                task_id_int = int(task_id_raw)
            except (TypeError, ValueError):
                continue

            task_obj, _ = Task.objects.get_or_create(task_id=task_id_int)

            # 用 annotator 當成這個使用者在此 task 下的「編號」
            try:
                annotation_id_int = int(annotator_raw) if annotator_raw is not None else 0
            except (TypeError, ValueError):
                annotation_id_int = 0

            # 同一個 Task + annotation_id 視為同一筆，重送會更新
            Annotation.objects.update_or_create(
                task=task_obj,
                annotation_id=annotation_id_int,
                defaults={
                    "rating": rating,
                    "relation": relation,
                },
            )
            created_or_updated += 1

        return JsonResponse({"errno": 0, "count": created_or_updated})
