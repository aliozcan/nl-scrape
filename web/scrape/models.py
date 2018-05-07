# This is an auto-generated Django model module created by ogrinspect.
from django.contrib.gis.db import models


class Country(models.Model):
    id_0 = models.BigIntegerField()
    iso = models.CharField(max_length=3)
    name_0 = models.CharField(max_length=75)
    objectid_1 = models.BigIntegerField()
    iso3 = models.CharField(max_length=5)
    name_engli = models.CharField(max_length=50)
    name_iso = models.CharField(max_length=54)
    name_fao = models.CharField(max_length=50)
    name_local = models.CharField(max_length=54)
    name_obsol = models.CharField(max_length=150)
    name_varia = models.CharField(max_length=160)
    name_nonla = models.CharField(max_length=50)
    name_frenc = models.CharField(max_length=50)
    name_spani = models.CharField(max_length=50)
    name_russi = models.CharField(max_length=50)
    name_arabi = models.CharField(max_length=50)
    name_chine = models.CharField(max_length=50)
    waspartof = models.CharField(max_length=100)
    contains = models.CharField(max_length=50)
    sovereign = models.CharField(max_length=40)
    iso2 = models.CharField(max_length=4)
    www = models.CharField(max_length=2)
    fips = models.CharField(max_length=6)
    ison = models.FloatField()
    validfr = models.CharField(max_length=12)
    validto = models.CharField(max_length=10)
    pop2000 = models.FloatField()
    sqkm = models.FloatField()
    popsqkm = models.FloatField()
    unregion1 = models.CharField(max_length=254)
    unregion2 = models.CharField(max_length=254)
    developing = models.FloatField()
    cis = models.FloatField()
    transition = models.FloatField()
    oecd = models.FloatField()
    wbregion = models.CharField(max_length=254)
    wbincome = models.CharField(max_length=254)
    wbdebt = models.CharField(max_length=254)
    wbother = models.CharField(max_length=254)
    ceeac = models.FloatField()
    cemac = models.FloatField()
    ceplg = models.FloatField()
    comesa = models.FloatField()
    eac = models.FloatField()
    ecowas = models.FloatField()
    igad = models.FloatField()
    ioc = models.FloatField()
    mru = models.FloatField()
    sacu = models.FloatField()
    uemoa = models.FloatField()
    uma = models.FloatField()
    palop = models.FloatField()
    parta = models.FloatField()
    cacm = models.FloatField()
    eurasec = models.FloatField()
    agadir = models.FloatField()
    saarc = models.FloatField()
    asean = models.FloatField()
    nafta = models.FloatField()
    gcc = models.FloatField()
    csn = models.FloatField()
    caricom = models.FloatField()
    eu = models.FloatField()
    can = models.FloatField()
    acp = models.FloatField()
    landlocked = models.FloatField()
    aosis = models.FloatField()
    sids = models.FloatField()
    islands = models.FloatField()
    ldc = models.FloatField()
    geom = models.MultiPolygonField(srid=4326)


# Auto-generated `LayerMapping` dictionary for Country model
country_mapping = {
    'id_0': 'ID_0',
    'iso': 'ISO',
    'name_0': 'NAME_0',
    'objectid_1': 'OBJECTID_1',
    'iso3': 'ISO3',
    'name_engli': 'NAME_ENGLI',
    'name_iso': 'NAME_ISO',
    'name_fao': 'NAME_FAO',
    'name_local': 'NAME_LOCAL',
    'name_obsol': 'NAME_OBSOL',
    'name_varia': 'NAME_VARIA',
    'name_nonla': 'NAME_NONLA',
    'name_frenc': 'NAME_FRENC',
    'name_spani': 'NAME_SPANI',
    'name_russi': 'NAME_RUSSI',
    'name_arabi': 'NAME_ARABI',
    'name_chine': 'NAME_CHINE',
    'waspartof': 'WASPARTOF',
    'contains': 'CONTAINS',
    'sovereign': 'SOVEREIGN',
    'iso2': 'ISO2',
    'www': 'WWW',
    'fips': 'FIPS',
    'ison': 'ISON',
    'validfr': 'VALIDFR',
    'validto': 'VALIDTO',
    'pop2000': 'POP2000',
    'sqkm': 'SQKM',
    'popsqkm': 'POPSQKM',
    'unregion1': 'UNREGION1',
    'unregion2': 'UNREGION2',
    'developing': 'DEVELOPING',
    'cis': 'CIS',
    'transition': 'Transition',
    'oecd': 'OECD',
    'wbregion': 'WBREGION',
    'wbincome': 'WBINCOME',
    'wbdebt': 'WBDEBT',
    'wbother': 'WBOTHER',
    'ceeac': 'CEEAC',
    'cemac': 'CEMAC',
    'ceplg': 'CEPLG',
    'comesa': 'COMESA',
    'eac': 'EAC',
    'ecowas': 'ECOWAS',
    'igad': 'IGAD',
    'ioc': 'IOC',
    'mru': 'MRU',
    'sacu': 'SACU',
    'uemoa': 'UEMOA',
    'uma': 'UMA',
    'palop': 'PALOP',
    'parta': 'PARTA',
    'cacm': 'CACM',
    'eurasec': 'EurAsEC',
    'agadir': 'Agadir',
    'saarc': 'SAARC',
    'asean': 'ASEAN',
    'nafta': 'NAFTA',
    'gcc': 'GCC',
    'csn': 'CSN',
    'caricom': 'CARICOM',
    'eu': 'EU',
    'can': 'CAN',
    'acp': 'ACP',
    'landlocked': 'Landlocked',
    'aosis': 'AOSIS',
    'sids': 'SIDS',
    'islands': 'Islands',
    'ldc': 'LDC',
    'geom': 'MULTIPOLYGON',
}


class Province(models.Model):
    id_0 = models.BigIntegerField()
    iso = models.CharField(max_length=3)
    name_0 = models.CharField(max_length=75)
    id_1 = models.BigIntegerField()
    name_1 = models.CharField(max_length=75)
    type_1 = models.CharField(max_length=50)
    engtype_1 = models.CharField(max_length=50)
    nl_name_1 = models.CharField(max_length=50)
    varname_1 = models.CharField(max_length=150)
    geom = models.MultiPolygonField(srid=4326)


# Auto-generated `LayerMapping` dictionary for Province model
province_mapping = {
    'id_0': 'ID_0',
    'iso': 'ISO',
    'name_0': 'NAME_0',
    'id_1': 'ID_1',
    'name_1': 'NAME_1',
    'type_1': 'TYPE_1',
    'engtype_1': 'ENGTYPE_1',
    'nl_name_1': 'NL_NAME_1',
    'varname_1': 'VARNAME_1',
    'geom': 'MULTIPOLYGON',
}



class Region(models.Model):
    id_0 = models.BigIntegerField()
    iso = models.CharField(max_length=3)
    name_0 = models.CharField(max_length=75)
    id_1 = models.BigIntegerField()
    name_1 = models.CharField(max_length=75)
    id_2 = models.BigIntegerField()
    name_2 = models.CharField(max_length=75)
    type_2 = models.CharField(max_length=50)
    engtype_2 = models.CharField(max_length=50)
    nl_name_2 = models.CharField(max_length=75)
    varname_2 = models.CharField(max_length=150)
    geom = models.MultiPolygonField(srid=4326)


# Auto-generated `LayerMapping` dictionary for Region model
region_mapping = {
    'id_0': 'ID_0',
    'iso': 'ISO',
    'name_0': 'NAME_0',
    'id_1': 'ID_1',
    'name_1': 'NAME_1',
    'id_2': 'ID_2',
    'name_2': 'NAME_2',
    'type_2': 'TYPE_2',
    'engtype_2': 'ENGTYPE_2',
    'nl_name_2': 'NL_NAME_2',
    'varname_2': 'VARNAME_2',
    'geom': 'MULTIPOLYGON',
}
