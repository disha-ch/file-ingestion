"""Document metadata model"""
from __future__ import annotations
from datetime import datetime
from typing import Dict, List, Optional, Literal, Any
from src.experiment import get_two_days_records
from src.logging import SingletonLogger

logger = SingletonLogger().get_logger()

class DocumentMetadata:
    table_name = "documents"

    def __init__(self, file_id: int, name: str, document_number: str, document_status: str, version_modified_date: datetime, file_created_date: Optional[datetime] = None,
                 pages: Optional[int] = None, major_version: Optional[int] = None, minor_version: Optional[int] = None,
                 language: Optional[str] = None, md5checksum: Optional[str] = None, country: Optional[str] = None,
                 gxp_category: Optional[str] = None, owning_business_area_1: Optional[List[str]] = None,
                 owning_business_area_2: Optional[List[str]] = None, owning_business_area_3: Optional[List[str]] = None,
                 owning_business_area_4: Optional[List[str]] = None, impacted_business_area_1: Optional[List[str]] = None,
                 impacted_business_area_2: Optional[List[str]] = None, impacted_business_area_3: Optional[List[str]] = None,
                 impacted_business_area_4: Optional[List[str]] = None, impacted_business_area_5: Optional[List[str]] = None,
                 impacted_business_area_6: Optional[List[str]] = None, product_family: Optional[str] = None,
                 product_variant: Optional[str] = None, material: Optional[str] = None, substance: Optional[str] = None,
                 material_group: Optional[str] = None, equipment_nongvlms: Optional[List[str]] = None,
                 entities_gvlms: Optional[List[str]] = None, equipment_type: Optional[List[str]] = None,
                 process_l1: Optional[List[str]] = None, process_l2: Optional[List[str]] = None, process_l3: Optional[List[str]] = None,
                 process_l4: Optional[List[str]] = None, process_l5: Optional[List[str]] = None, status: str = "DOWNLOADING"):
        self.file_id = file_id
        self.name = name
        self.document_number = document_number
        self.document_status = document_status
        self.version_modified_date = version_modified_date
        self.file_created_date = file_created_date
        self.pages = pages
        self.major_version = major_version
        self.minor_version = minor_version
        self.language = language
        self.md5checksum = md5checksum
        self.country = country
        self.gxp_category = gxp_category
        self.owning_business_area_1 = owning_business_area_1
        self.owning_business_area_2 = owning_business_area_2
        self.owning_business_area_3 = owning_business_area_3
        self.owning_business_area_4 = owning_business_area_4
        self.impacted_business_area_1 = impacted_business_area_1
        self.impacted_business_area_2 = impacted_business_area_2
        self.impacted_business_area_3 = impacted_business_area_3
        self.impacted_business_area_4 = impacted_business_area_4
        self.impacted_business_area_5 = impacted_business_area_5
        self.impacted_business_area_6 = impacted_business_area_6
        self.product_family = product_family
        self.product_variant = product_variant
        self.material = material
        self.substance = substance
        self.material_group = material_group
        self.equipment_nongvlms = equipment_nongvlms
        self.entities_gvlms = entities_gvlms
        self.equipment_type = equipment_type
        self.process_l1 = process_l1
        self.process_l2 = process_l2
        self.process_l3 = process_l3
        self.process_l4 = process_l4
        self.process_l5 = process_l5
        self.status = status

    @classmethod
    def get_query(cls, execution_type: Literal["Incremental", "Load"]) -> str:
        base = (
            "SELECT id, name__v, file_created_date__v, version_modified_date__v, status__v, pages__v, "
            "major_version_number__v, minor_version_number__v, language__v, md5checksum__v, country__v, "
            "gxp_category__c, product_family__c, product_variant__c, material__c, substance__c, material_group__c, "
            "owning_business_area_1__c, owning_business_area_2__c, owning_business_area_3__c, owning_business_area_4__c, "
            "impacted_business_area_1__c, impacted_business_area_2__c, impacted_business_area_3__c, impacted_business_area_4__c, "
            "impacted_business_area_5__c, impacted_business_area_6__c, equipment_nongvlms__c, entities_gvlms__c, equipment_type__c, "
            "process_l1__c, process_l2__c, process_l3__c, process_l4__c, process_l5__c, document_number__v "
            "FROM ALLVERSIONS documents WHERE (status__v  = 'Effective') "
            "AND (type__v IN ('Work Instruction','Standard Operating Procedure (SOP)','Standard','Form','Template','Guidance')) "
            "AND security__c = 'Open' "
        )
        if execution_type == "Incremental":
            base += f" AND version_modified_date__v >= '{get_two_days_records()}'"
        return base

    @classmethod
    def get_query_single_document(cls, document_number: str) -> str:
        return (
            "SELECT id, name__v, file_created_date__v, version_modified_date__v, pages__v, status__v, major_version_number__v, "
            "minor_version_number__v, language__v, md5checksum__v, country__v, gxp_category__c, product_family__c, product_variant__c, "
            "material__c, substance__c, material_group__c, owning_business_area_1__c, owning_business_area_2__c, owning_business_area_3__c, "
            "owning_business_area_4__c, impacted_business_area_1__c, impacted_business_area_2__c, impacted_business_area_3__c, "
            "impacted_business_area_4__c, impacted_business_area_5__c, impacted_business_area_6__c, equipment_nongvlms__c, entities_gvlms__c, "
            "equipment_type__c, process_l1__c, process_l2__c, process_l3__c, process_l4__c, process_l5__c, document_number__v "
            "FROM documents WHERE (status__v  = 'Effective') AND latest_version__v = true AND security__c = 'Open' "
            f"AND document_number__v = '{document_number}'"
        )

    @classmethod
    def model_validate(cls, api_response: Dict[str, Any], **kwargs) -> "DocumentMetadata":
        # Normalize impacted/owning fields into lists
        def norm_field(name):
            v = api_response.get(name)
            if v is None:
                return []
            if isinstance(v, list):
                return v
            return [v]

        impacted = [norm_field(f"impacted_business_area_{i}__c") for i in range(1, 7)]
        owning = [norm_field(f"owning_business_area_{i}__c") for i in range(1, 5)]

        def parse_dt(key):
            val = api_response.get(key)
            if not val:
                return None
            return datetime.strptime(val, "%Y-%m-%dT%H:%M:%S.%fZ")

        return cls(
            file_id=int(api_response["id"]),
            name=api_response.get("name__v", ""),
            document_number=api_response.get("document_number__v", ""),
            document_status=api_response.get("status__v", ""),
            file_created_date=parse_dt("file_created_date__v"),
            version_modified_date=parse_dt("version_modified_date__v"),
            pages=int(api_response.get("pages__v")) if api_response.get("pages__v") else None,
            major_version=int(api_response.get("major_version_number__v")) if api_response.get("major_version_number__v") else None,
            minor_version=int(api_response.get("minor_version_number__v")) if api_response.get("minor_version_number__v") else 0,
            language=api_response.get("language__v"),
            md5checksum=api_response.get("md5checksum__v"),
            country=api_response.get("country__v"),
            gxp_category=api_response.get("gxp_category__c"),
            owning_business_area_1=owning[0],
            owning_business_area_2=owning[1],
            owning_business_area_3=owning[2],
            owning_business_area_4=owning[3],
            impacted_business_area_1=impacted[0],
            impacted_business_area_2=impacted[1],
            impacted_business_area_3=impacted[2],
            impacted_business_area_4=impacted[3],
            impacted_business_area_5=impacted[4],
            impacted_business_area_6=impacted[5],
            equipment_nongvlms=api_response.get("equipment_nongvlms__c"),
            entities_gvlms=api_response.get("entities_gvlms__c"),
            equipment_type=api_response.get("equipment_type__c"),
            process_l1=api_response.get("process_l1__c"),
            process_l2=api_response.get("process_l2__c"),
            process_l3=api_response.get("process_l3__c"),
            process_l4=api_response.get("process_l4__c"),
            process_l5=api_response.get("process_l5__c"),
        )

    def get_document_id(self) -> str:
        return '{"id": "' + str(self.file_id) + '"}'

    def rename_relations(self, **mappings) -> None:
        def map_values(values, mapping):
            return [mapping.get(x, x) for x in values] if values else values

        for attr, mapping in mappings.items():
            if hasattr(self, attr):
                v = getattr(self, attr)
                if isinstance(v, list):
                    setattr(self, attr, map_values(v, mapping))
                else:
                    setattr(self, attr, mapping.get(v, v))

    def model_dump(self) -> Dict:
        def iso(dt):
            return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ") if dt else None

        return {
            "file_id": str(self.file_id),
            "name": self.name,
            "document_number": self.document_number,
            "document_status": self.document_status,
            "timestamp": iso(self.version_modified_date),
            "file_created_date": iso(self.file_created_date),
            "major_version": str(self.major_version),
            "minor_version": str(self.minor_version),
            "pages": str(self.pages) if self.pages is not None else None,
            "language": self.language,
            "md5": self.md5checksum,
            "country": self.country,
            "status": self.status,
            "product_family": self.product_family,
        }

    @staticmethod
    def filter_by_impacted_business_area(model, impacted_business_areas, required_impacted_areas=2):
        def check_if_contains(possible_values, real_values):
            if not real_values and "" in possible_values:
                return True
            for r in real_values:
                if r in possible_values:
                    return True
            return False

        assert len(impacted_business_areas) == 6
        for idx, possible_values in enumerate(impacted_business_areas):
            real_values = getattr(model, f"impacted_business_area_{idx+1}")
            if idx < required_impacted_areas:
                if not real_values or not check_if_contains(possible_values, real_values):
                    return False
            if real_values and not check_if_contains(possible_values, real_values) and len(possible_values) != 0:
                return False
        return True
