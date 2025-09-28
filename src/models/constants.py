from typing import Dict, Literal, Any
from src.experiment import get_two_days_records

class Constant:
    table_name: str

    def __init__(self, id: str, name: str):
        self.id = id
        self.name = name

    @classmethod
    def get_query(cls, _: Literal["Incremental", "Load"]) -> str:
        return f"SELECT id, name__v FROM {cls.table_name} WHERE modified_date__v >= '{get_two_days_records()}'"

    @classmethod
    def model_validate(cls, api_response: Dict[str, str], **kwargs) -> "Constant":
        return cls(id=api_response["id"], name=api_response["name__v"])

    def __repr__(self) -> str:
        return f"{self.id}: {self.name}"

# Concrete classes
class Country(Constant): table_name = "country__v"
class ObjectReference(Constant): table_name = "object_reference_field_value__c"
class BusinessArea1(Constant): table_name = "business_area_1__c"
class BusinessArea2(Constant): table_name = "business_area_2__c"
class BusinessArea3(Constant): table_name = "qms_organization__qdm"
class BusinessArea4(Constant): table_name = "department__v"
class BusinessArea5(Constant): table_name = "business_area_5__c"
class BusinessArea6(Constant): table_name = "business_area_6__c"
class ProductFamily(Constant): table_name = "product_family__v"
class ProductVariant(Constant): table_name = "product_variant__v"
class MaterialGroup(Constant): table_name = "material_group__c"
class SubstanceMaterialEquipment(Constant): table_name = "context__qdm"
class DeviceFamily(Constant): table_name = "device_family__c"
class Equipment(Constant): table_name = "equipment__c"
class EquipmentType(Constant): table_name = "equipment_type__c"
class BusinessProcessL1(Constant): table_name = "business_process__v"
class BusinessProcessL2(Constant): table_name = "process_level_2__c"
class BusinessProcessL3(Constant): table_name = "process_level_3__c"
class BusinessProcessL4(Constant): table_name = "process_level_4__c"
class BusinessProcessL5(Constant): table_name = "process_level_5__c"

# SOP selector
class SOPs(Constant):
    table_name = "documents"

    @classmethod
    def get_query(cls, _: Literal["Incremental", "Load"]) -> str:
        return (
            f"SELECT id, document_number__v FROM {cls.table_name} WHERE type__v = 'Standard Operating Procedure (SOP)' "
            "AND security__c = 'Open' AND latest_version__v = true"
        )

    @classmethod
    def model_validate(cls, api_response: Dict[str, str], **kwargs) -> "Constant":
        return cls(id=api_response["id"], name=api_response["document_number__v"])
