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
import re
import json
from django.conf import settings
from .models import Task, Annotation, Dataset


MAX_WORKERS = 8
LS_URL = getattr(settings, "LABEL_STUDIO_URL")            # 例如: "https://app.humansignal.com"
LS_TOKEN = getattr(settings, "LABEL_STUDIO_TOKEN")        # 這是你的 PAT（Personal Access Token）
PROJECT_ID = int(getattr(settings, "PROJECT_ID"))
MY_UID = int(getattr(settings, "MY_UID"))
total = int(getattr(settings, "TOTAL"))
ALLOWED_REL = {'E', 'S', 'C', 'I'}  # ESCI
FETCH_NUM = 100
task_ids = []


def get_data(annotator=None):
    """
    從 SQLite 的 Dataset 取出 rows。
    annotator 不為 None 時，只保留有該 annotator 的樣本
    （Dataset.annotator 以逗號串接，例如 "1,4,7,11"）。
    """
    result = []
    annotator_str = str(annotator) if annotator is not None else None

    qs = Dataset.objects.all().order_by("task_id")

    for obj in qs:
        raw_annotators = obj.annotator or ""
        annotator_list = [a for a in raw_annotators.split(",") if a]

        # 有指定 annotator：只保留有這個 id 的樣本
        if annotator_str is not None and annotator_str not in annotator_list:
            continue

        row_dict = {
            "id": str(obj.task_id),
            "IT_NAME": obj.it_name,
            "image_url": obj.img,
            "query": obj.query,
            # 轉成 list，等等會塞成 list[dict]
            "annotators": annotator_list,
        }
        result.append(row_dict)

    return result


@csrf_exempt
def index(request):
    if request.method == "GET":
        # ?annotator=1~12，若沒給或是 all 則不篩選
        annotator = request.GET.get("annotator")
        if annotator in ("", None, "all"):
            annotator = None

        want_json = (
            request.headers.get("x-requested-with") == "XMLHttpRequest"
            or request.GET.get("format") == "json"
        )

        # 一般頁面載入：只回傳樣板，資料改由前端 AJAX 取得
        if not want_json:
            return render(
                request,
                "tables.html",
                {
                    "annotator": annotator,
                    "next_task_id": 223471874,
                },
            )

        # AJAX / JSON 取得資料
        rows = get_data(annotator=annotator)

        # 先收集所有需要查的 task_id，一次查 DB 避免 N+1
        task_ids = []
        for row in rows:
            task_id_str = row.get("id")
            try:
                task_id_int = int(task_id_str)
            except (TypeError, ValueError):
                continue
            task_ids.append(task_id_int)

        task_ids = list(set(task_ids))  # 去重

        # 一次把所有 Task + annotations 撈回來
        tasks = (
            Task.objects.filter(task_id__in=task_ids).prefetch_related("annotations")
        )
        task_map = {t.task_id: t for t in tasks}

        # 把 annotation 資訊補回每一列
        for row in rows:
            task_id_str = row.get("id")
            try:
                task_id_int = int(task_id_str)
            except (TypeError, ValueError):
                continue

            task = task_map.get(task_id_int)
            if not task:
                continue

            # 建立 { annotator: (rating, relation) } map
            ann_map = {
                str(a.annotation_id): (a.rating, a.relation)
                for a in task.annotations.all()
            }

            original_ann_list = row.get("annotators", [])
            new_ann_list = []

            # original_ann_list 現在是像 ['1','4','7'] 這種
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

        return JsonResponse(
            {
                "rows": rows,
                "annotator": annotator,
                "next_task_id": 223471874,
            },
            json_dumps_params={"ensure_ascii": False},
        )

    if request.method == "POST":
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
