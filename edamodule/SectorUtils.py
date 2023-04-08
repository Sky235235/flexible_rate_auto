import pandas as pd
from pyproj import Transformer
from tqdm import tqdm

class GetCoordinate:
    def __init__(self):
        self.wgs84 = {'proj': 'latlong', 'datum': 'WGS84', 'ellps': 'WGS84'}
        self.tm128 = {'proj': 'tmerc', 'lat_0': '38N', 'lon_0': '128E', 'ellps': 'bessel',
                 'x_0': '400000', 'y_0': '600000', 'k': '0.9999', 'towgs84': '-146.43,507.89,681.46'}

    def wgstokatec(self, lng, lat):
        '''lng -> List, lat -> lst'''
        _WGS84 = self.wgs84
        _TM128 = self.tm128
        _transformer = Transformer.from_crs((_WGS84), (_TM128))
        converted = _transformer.transform(lng, lat)
        return converted

    def katectowgs(self, xpos, ypos):
        ''' xpos ->list , ypos->list'''
        _WGS84 = self.wgs84
        _TM128 = self.tm128
        _transformer = Transformer.from_crs((_TM128),(_WGS84))
        converted = _transformer.transform(xpos, ypos)
        return converted
    
    def get_sector(self, df, xpos, ypos):
        '''df -> DataFrame,
           xpos -> colname , string,
           ypos -> colname , string'''
        _left_xpos = 290830.5213
        _bottom_ypos = 536239.3348

        _xpos_list = ((df[xpos] - _left_xpos) // 1000) * 100
        _ypos_list = (df[ypos] - _bottom_ypos) // 1000
        ## 섹터 외부에 있는 부분 변경
        _sector_list = []
        for x, y in zip(_xpos_list, _ypos_list):
            if (x < 0) | (x > 3700) | (y < 0) | (y > 30):
                _sector_list.append(9999)
            else:
                define_sector = x+y
                _sector_list.append(define_sector)
        sector_list = _sector_list
        return sector_list



