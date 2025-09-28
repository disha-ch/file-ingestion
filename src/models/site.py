from enum import Enum

class Site(str, Enum):
    SWEDEN_STERILE = "sweden_sterile"
    SWEDEN_SBC = "sweden_sbc"
    SWEDEN_OSD = "sweden_osd"
    US_MOUNT_VERMONT = "us_mount_vermont"
    CHINA_TAIZHOU = "china_thaizou"
    CHINA_WUXI = "china_wuxi"
