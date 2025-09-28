from typing import Dict, Literal
from src.experiment import get_two_days_records

class WithdrawnDocument:
    table_name = "documents"

    def __init__(self, file_id: str, name: str):
        self.file_id = file_id
        self.name = name

    @classmethod
    def get_query(cls, _: Literal["Incremental", "Load"]) -> str:
        return (
            "SELECT id, name__v FROM documents WHERE (status__v = 'Withdrawn' OR status__v = 'Superseded') "
            "AND (type__v IN ('Work Instruction','Standard Operating Procedure (SOP)','Standard','Form','Template','Guidance')) "
            "AND latest_version__v = true AND security__c = 'Open' "
            f"AND version_modified_date__v >= '{get_two_days_records()}'"
        )

    @classmethod
    def model_validate(cls, api_response: Dict[str, str], **kwargs) -> "WithdrawnDocument":
        return cls(file_id=api_response["id"], name=api_response["name__v"])

    def __repr__(self) -> str:
        return f"{self.file_id}: {self.name}"
