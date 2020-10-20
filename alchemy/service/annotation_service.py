"""Service that handles creating, reading, updating and deleting annotations"""
from typing import List

from flask import Blueprint, request

from alchemy.dao.annotation_dao import AnnotationDao
from alchemy.data.pojo import AnnotationCreationRequest


bp = Blueprint("annotation_service", __name__, url_prefix="/annotation_service")


class AnnotationService:
    def __init__(self, service_dao: AnnotationDao):
        self.service_dao = service_dao

    def create_annotation(self, create_request: AnnotationCreationRequest):
        self.service_dao.create_annotation(create_request)

    @bp.route("/bulk", methods=["POST"])
    def create_annotations_bulk(self, create_requests: List[AnnotationCreationRequest]):
        # TODO evaluate bear token
        # TODO evaluate if the data in the JSON object satisfy the requirements
        self.service_dao.create_annotations_bulk(create_requests)

    def retrieve_annotations(self):
        pass

    def delete_annotations(self):
        pass
