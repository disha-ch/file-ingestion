from dataclasses import dataclass
from typing import List
from src.models.site import Site
from src.models.language import Language

@dataclass
class Question:
    Site: Site
    Language: Language
    Query: str
    Expected: str

@dataclass
class ListQuestion:
    questions: List[Question]
