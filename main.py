import requests
import json
import time
import datetime
from scipy import optimize
from bs4 import BeautifulSoup
requests.packages.urllib3.disable_warnings()

Cookie = "..."


class Xirr(object):
    def __init__(self, fund):
        self.fund = fund

    @staticmethod
    def _xirr(data, guess=0.1):
        def xnpv(rate, cashflows):
            return sum([cf / (1 + rate) ** ((t - cashflows[0][0]).days / 365.0) for (t, cf) in cashflows])

        try:
            return round(optimize.newton(lambda r: xnpv(r, data), guess) * 100, 2)
        except Exception as e:
            raise Exception('Calc Wrong')

    def calc_rate(self):
        data = self.fund.handler.get_xirr_data()
        data.append((datetime.datetime.now().date(), self.fund.total_assets))
        return self._xirr(data)


class DanjuanFundHandle(object):
    def __init__(self, fd):
        """ 读取详细页初始化实例属性 """
        self.fd = fd
        data = self.get_summary_data()
        fd.parse_summary_data(data)

    @staticmethod
    def get_api_data(url):
        headers = {
            "User-Agent": "Booking.App/18.3.1 iOS/12.1.1; Type: phone; AppStore: apple; Brand: Apple; Model: iPhone9,2;",
            "Accept": "*/*",
            "Cookie": Cookie,
            "Connection": "keep-alive"
        }
        rsp = json.loads(requests.get(url, headers=headers).text)
        # {'result_code': 300001, 'message': '请重新登录'}
        if rsp["result_code"] != 0:
            raise Exception(rsp["message"])
        return rsp["data"]

    def get_trade_list_data(self, page=1):
        """ 遍历交易记录 """
        ret = []
        data = self.get_api_data(self.fd.url_for_trade_list(page=page))
        if data["total_items"] != 0:
            ret = data["items"]
            time.sleep(0.1)
            ret.extend(self.get_trade_list_data(page=page + 1))
        return ret

    def get_summary_data(self):
        """ 获得汇总数据 """
        data = self.get_api_data(self.fd.url_for_detail())
        return data

    def get_sell_data(self, oid):
        """ 获得指定订单的卖出数据 """
        data = self.get_api_data(self.fd.url_for_order(oid))
        if data["status"] != "success":
            return 0
        return data[self.fd.field_for_sell_amount]

    def parse_data_for_xirr(self, data):
        # 买入 action='022'
        # 卖出 action='024'
        # 分红 action='143'
        ret = []
        for i in data:
            # if ("success" not in i.get("status", "")) and ("wait" not in i.get("status", "")):
            if i.get("status_desc", "") not in ["交易成功", "交易进行中"]:
                continue
            # if "份" in i.get("value_desc", ""):
            #     continue

            timestamp, _ = divmod(i["created_at"], 1000)
            dt = datetime.datetime.fromtimestamp(timestamp).date()
            if i["action"] == '022':
                # 买入操作
                money = -1 * i["amount"]
            elif i["action"] == '024':
                # 卖出操作
                money = self.get_sell_data(i["order_id"])
            elif i["action"] == '143' and "元" in i["value_desc"]:
                # 现金分红
                money = i["amount"]
            else:
                continue
            ret.append((dt, money))
            ret.sort()
        return ret

    def get_xirr_data(self):
        return self.parse_data_for_xirr(self.get_trade_list_data())


class RuiyuanFundHandle(object):
    def __init__(self, fd):
        """ 读取详细页初始化实例属性 """
        self.fd = fd
        data = self.get_summary_data()
        fd.parse_summary_data(data)

    @staticmethod
    def get_api_data(url, data=None):
        headers = {
            "User-Agent": "Booking.App/18.3.1 iOS/12.1.1; Type: phone; AppStore: apple; Brand: Apple; Model: iPhone9,2;",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "*/*",
            "Cookie": Cookie,
            "Connection": "keep-alive"
        }

        rsp = requests.post(url, headers=headers, data=data, verify=False)
        soup = BeautifulSoup(rsp.text, 'lxml')
        if rsp.status_code == 500:
            raise Exception(soup.find(id='errormsg')['value'])
        return soup

    def get_trade_list_data(self, page=1, size=100):
        """ 遍历交易记录 """
        payload = {
            'page_no': page,
            'page_size': size,
            'agency_type': 1,
            'begin_date': "{:%Y-%m-%d}".format((datetime.datetime.now() - datetime.timedelta(days=365))),
            'end_date': "{:%Y-%m-%d}".format(datetime.datetime.now()),
            'fund_busin_code': '',
            'fund_code': self.fd.pk
        }
        data = self.get_api_data(self.fd.url_for_trade_list(), data=payload)
        # 获得总记录数
        total = int(data.find("div", {"class": "pagination"})["totalcount"])
        ret = json.loads(data.find(id="dcDataJson")["value"])
        if page * size < total:
            time.sleep(0.1)
            ret.extend(self.get_trade_list_data(page=page + 1))
        return ret

    def get_summary_data(self):
        """ 获得汇总数据 """
        payload = {
            'agency_type': 1
        }
        data = self.get_api_data(self.fd.url_for_detail(), data=payload)
        return data

    def parse_data_for_xirr(self, data):
        # 买入 action='022'
        # 卖出 action='024'
        # 分红 action='143'
        ret = []
        for i in data:
            # if ("success" not in i.get("status", "")) and ("wait" not in i.get("status", "")):
            if i.get("fund_code") != self.fd.pk:
                continue
            if i.get("trade_status_name", "") not in ["确认成功"]:
                continue

            # if "份" in i.get("value_desc", ""):
            #     continue

            dt = datetime.datetime.strptime(i["apply_date"], "%Y%m%d").date()
            if i["busin_flag"] in ['02', '50']:
                # 买入操作
                money = -1 * float(i["trade_confirm_balance"])
            else:
                continue
            ret.append((dt, money))
            ret.sort()
        return ret

    def get_xirr_data(self):
        return self.parse_data_for_xirr(self.get_trade_list_data())


class Fund(object):
    def __init__(self, pk, **kw):
        self.pk = pk
        self._name = None       # 基金名称
        self._total_assets = 0  # 当前市值

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def total_assets(self):
        return self._total_assets

    @total_assets.setter
    def total_assets(self, n):
        self._total_assets = n


class DanjuanFund(Fund):
    def __init__(self, pk, pid=None):
        super(DanjuanFund, self).__init__(pk)
        self.pid = pid
        self.handler = DanjuanFundHandle(self)

    @property
    def field_for_sell_amount(self):
        return "confirm_amount"

    def url_for_detail(self):
        if self.pid is not None:
            return "https://danjuanapp.com/djapi/holding/plan/item?plan_code={}&fd_code={}".format(self.pid, self.pk)
        return "https://danjuanapp.com/djapi/holding/fund/{}".format(self.pk)

    def url_for_order(self, oid):
        return "https://danjuanapp.com/djapi/plan/order/{}".format(oid)

    def url_for_trade_list(self, page=1, size=20):
        if self.pid is not None:
            return "https://danjuanapp.com/djapi/order/{}/{}/trade/list?page={}&size={}".format(self.pid, self.pk, page, size)
        return "https://danjuanapp.com/djapi/order/p/{}/list?page={}&size={}&type=all".format(self.pk, page, size)

    def parse_summary_data(self, data):
        """ 汇总页的数据解析规则 """
        self.name = data["fd_name"]
        self.total_assets = data["market_value"]


class DanjuanPlan(Fund):
    """ 基金组合 """
    def __init__(self, pk):
        super(DanjuanPlan, self).__init__(pk)
        self.funds = []
        self.handler = DanjuanFundHandle(self)

    @property
    def field_for_sell_amount(self):
        return "total_confirm_amount"

    def url_for_detail(self):
        return "https://danjuanapp.com/djapi/holding/plan/{}".format(self.pk)

    def url_for_order(self, oid):
        return "https://danjuanapp.com/djapi/order/p/plan/{}".format(oid)

    def url_for_trade_list(self, page=1, size=20):
        return "https://danjuanapp.com/djapi/order/p/{}/list?page={}&size={}&type=all".format(self.pk, page, size)

    def parse_summary_data(self, data):
        """ 汇总页的数据解析规则 """
        self.name = data["name"]
        self.total_assets = data["total_assets"]
        for i in data.get("items", []):
            self.funds.append(DanjuanFund(i["fd_code"], pid=self.pk))


class RuiyuanFund(Fund):
    def __init__(self, pk):
        super(RuiyuanFund, self).__init__(pk)
        self.handler = RuiyuanFundHandle(self)

    def url_for_detail(self):
        return "https://etrading.foresightfund.com/etrading/account/main/querydcshare"

    def url_for_trade_list(self):
        return "https://etrading.foresightfund.com/etrading/query/main/list"

    def parse_summary_data(self, data):
        for i in json.loads(data.find(id="agencyDataJson")["value"]):
            if i.get("out_fund_code", "") == self.pk:
                self.name = i.get("out_fund_name", "")
                self.total_assets = float(i.get("worth_value", 0))


def main(*funds):
    total = 0
    print("截止 {:%Y-%m-%d}".format(datetime.datetime.now()))
    pret = []
    fret = []
    for fund in funds:
        if hasattr(fund, "funds"):
            for f in fund.funds:
                x = Xirr(f)
                fret.append((x.calc_rate(), int(f.total_assets), f.pk, f.name))
            fret.sort(reverse=True)
        x = Xirr(fund)
        pret.append((x.calc_rate(), int(fund.total_assets), fund.pk, fund.name))
        total += int(fund.total_assets)

    for i in fret:
        print("年化收益率: {:>5.2f}%  市值: {:>6d}  基金代码 {:>6s} {:<30s}".format(i[0], i[1], i[2], i[3]))

    print()
    for i in pret:
        print("年化收益率: {:>5.2f}%  市值: {:>6d}  基金代码 {:>6s} {:<30s}".format(i[0], i[1], i[2], i[3]))

    print("\n总市值: {}".format(total))


if __name__ == "__main__":
    # RuiyuanFund("007119")
    main(DanjuanPlan("CSI666"))
