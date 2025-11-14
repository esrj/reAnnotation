from django.db import models


class Task(models.Model):
    task_id = models.BigIntegerField(unique=True)
    def __str__(self):
        return str(self.task_id)


class Annotation(models.Model):
    RATING_CHOICES = [(i, str(i)) for i in range(5)]
    RELATION_CHOICES = [
        ("E", "E"),
        ("S", "S"),
        ("C", "C"),
        ("I", "I"),
    ]

    # 一個 Task 對多個 Annotation
    task = models.ForeignKey(Task, related_name="annotations", on_delete=models.CASCADE)

    # 單筆標註在外部系統的 id
    annotation_id = models.BigIntegerField()
    rating = models.IntegerField(choices=RATING_CHOICES)
    relation = models.CharField(max_length=1, choices=RELATION_CHOICES)

    class Meta:
        indexes = [
            models.Index(fields=["annotation_id"]),
            models.Index(fields=["task"]),
        ]

    def __str__(self):
        return f"{self.annotation_id} ({self.rating}, {self.relation})"
