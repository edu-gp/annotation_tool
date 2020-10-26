"""Service that handles creating, reading, updating and deleting annotations"""
from typing import List

from alchemy.dao.annotation_dao import AnnotationDao
from alchemy.data.request.annotation_request import AnnotationCreateRequest


class AnnotationService:
    def __init__(self, service_dao: AnnotationDao):
        self.service_dao = service_dao

    def create_annotation(self, create_request: AnnotationCreateRequest):
        # self.service_dao.create_annotation(create_request)
        pass

    def create_annotations_bulk(self, create_requests: List[AnnotationCreateRequest]):
        # self.service_dao.create_annotations_bulk(create_requests)
        pass

    def retrieve_annotations(self):
        pass

    def delete_annotations(self):
        pass
