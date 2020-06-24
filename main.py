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
        self.trade_list = fund.handler.get_xirr_data()

    @staticmethod
    def _xirr(cashflows):
        years = [(ta[0] - cashflows[0][0]).days / 365. for ta in cashflows]
        residual = 1.0
        step = 0.05
        guess = 0.05
        epsilon = 0.0001
        limit = 10000
        while abs(residual) > epsilon and limit > 0:
            limit -= 1
            residual = 0.0
            for i, trans in enumerate(cashflows):
                residual += trans[1] / pow(guess, years[i])
            if abs(residual) > epsilon:
                if residual > 0:
                    guess += step
                else:
                    guess -= step
                    step /= 2.0
        return round((guess - 1) * 100, 2)

    def calc_rate(self):
        data = []
        for i in self.trade_list:
            data.append((i[0], i[1]))

        if len(data) == 0:
            return 0

        if len(data) == 1 and data[0][0] == datetime.datetime.now().date():
            return 0

        if data[0][0] + datetime.timedelta(days=365) < datetime.datetime.now().date():
            dt = datetime.datetime.now().date()
        else:
            dt = data[0][0] + datetime.timedelta(days=365)
        data.append((dt, self.fund.total_assets))
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
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
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
        # print(self.fd.name, ret)
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
                desc = i['action_desc']
                source = "蛋卷"
            elif i["action"] == '024':
                # 卖出操作
                money = self.get_sell_data(i["order_id"])
                desc = i['action_desc']
                source = "蛋卷"
            elif i["action"] == '143' and "元" in i["value_desc"]:
                # 现金分红
                money = i["amount"]
                desc = i['action_desc']
                source = "蛋卷"
            else:
                continue
            ret.append((dt, money, desc, source))
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
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "*/*",
            "Cookie": "JSESSIONID=D3475DC7E5F289735E82B3617EDC9226; Hm_lvt_9e73ae371b4412c018107ac556ded455=1582079037,1582681550,1582781336,1584324709; Hm_lpvt_9e73ae371b4412c018107ac556ded455=1584324709; SL_GWPT_Show_Hide_tmp=1; SL_wptGlobTipTmp=1",
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
            'begin_date': "{:%Y-%m-%d}".format((datetime.datetime.now() - datetime.timedelta(days=730))),
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
            if i["busin_flag"] in ['02', '50', '39']:
                # 买入操作(认购/申购/定投)
                money = -1 * float(i["trade_confirm_balance"])
                desc = i["busin_name"]
                source = i["agency_name"]
            else:
                continue
            ret.append((dt, money, desc, source))
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


class BoshiFundHandle(object):
    def __init__(self, fd):
        """ 读取详细页初始化实例属性 """
        self.fd = fd
        data = self.get_summary_data()
        fd.parse_summary_data(data)

    @staticmethod
    def get_api_data(url, data=None):
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Cookie": "JSESSIONID=0000mx_FTykLTK2eFA0m8a1F3Q7:-1; Hm_lvt_3c5b5c9332a21f25e5643038564d17c1=1577795402; SL_GWPT_Show_Hide_tmp=1; SL_wptGlobTipTmp=1; 445c77cb0f12be51ac6cec72f00dfecd=0950cbbf625ddfa1bf64ace612a4da92; Hm_lpvt_3c5b5c9332a21f25e5643038564d17c1=1577936059; OZ_1U_2105=vid=ve0d64a88f3b80.0&ctime=1577936109&ltime=1577936108; OZ_1Y_2105=erefer=-&eurl=https%3A//trade.bosera.com/&etime=1577936040&ctime=1577936109&ltime=1577936108&compid=2105",
            "Connection": "keep-alive"
        }
        rsp = json.loads(requests.post(url, headers=headers, data=data, verify=False).text)
        # {'result_code': 300001, 'message': '请重新登录'}
        if rsp["retCode"] != "0":
            raise Exception(rsp["errMsg"])
        return rsp["data"]

    def get_dividend_data(self, page=1):
        payload = {
            'dateRange': 'sdd',
            'page': page,
            'startDate': "2000-01-01",
            'endDate': "{:%Y-%m-%d}".format(datetime.datetime.now())
        }
        data = self.get_api_data(self.fd.url_for_dividend(), data=payload)
        ret = [i for i in data['resultList'] if i['fundCode'] == self.fd.pk]
        if page < data['paginator']['total']:
            time.sleep(0.1)
            ret.extend(self.get_dividend_data(page=page + 1))
        return ret

    def get_trade_list_data(self, page=1):
        """ 遍历交易记录 """
        payload = {
            'dateRange': 'sdd',
            'productType': 'openfund',
            'checkProduct': 'Y',
            'page': page,
            'agency': 'agent',
            'startDate': "2000-01-01",
            'endDate': "{:%Y-%m-%d}".format(datetime.datetime.now()),
            'product': self.fd.pk
        }
        data = self.get_api_data(self.fd.url_for_trade_list(), data=payload)
        # 获得总记录数
        ret = data['resultList']
        if page == 1:
            ret.extend(self.get_dividend_data())
        if data['paginator']['page'] < data['paginator']['total']:
            time.sleep(0.1)
            ret.extend(self.get_trade_list_data(page=page + 1))
        return ret

    def get_summary_data(self):
        """ 获得汇总数据 """
        payload = {
            'timeScope': 1,
            'page': 1,
            'startDate': "{:%Y-%m-%d}".format((datetime.datetime.now() - datetime.timedelta(days=30))),
            'endDate': "{:%Y-%m-%d}".format(datetime.datetime.now()),
            'year': "{:%Y}".format(datetime.datetime.now()),
            'halfYear': 'first',
            'season': 1,
            'month': 12
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
            if i.get("fundCode") != self.fd.pk:
                continue
            if i.get("showStatus", "") not in ["成功", ""]:
                continue

            # if "份" in i.get("value_desc", ""):
            #     continue
            dt = datetime.datetime.strptime(i.get("transactionDate") or i.get("transactionCfmDate"), "%Y-%m-%d").date()
            if i.get("transactionTypeName", "") in ['申购', '转换转入', '认购结果']:
                # 买入操作
                money = -1 * float(i["applicationAmount"].replace(",", ""))
                desc = i["transactionTypeName"]
                source = i["distributorName"]
            elif i.get("transactionTypeName", "") in ['赎回', '转换转出']:
                money = float(i["applicationAmount"].replace(",", ""))
                desc = i["transactionTypeName"]
                source = i["distributorName"]
            elif i.get("dividendPerUnit"):
                money = float(i["confirmedAmount"].replace(",", ""))
                if money == 0:
                    continue
                desc = "分红"
                source = i.get("payTypeShowStr", "")
            else:
                # print(i)
                continue
            ret.append((dt, money, desc, source))
        ret.sort()
        return ret

    def get_xirr_data(self):
        return self.parse_data_for_xirr(self.get_trade_list_data())


class BoshiFund(Fund):
    def __init__(self, pk, pid=None):
        super(BoshiFund, self).__init__(pk)
        self.pid = pid
        self.handler = BoshiFundHandle(self)

    def url_for_detail(self):
        return "https://trade.bosera.com/acctQry/profitDetailFund.json"

    def url_for_trade_list(self):
        return "https://trade.bosera.com/acctQry/tradeRecords.json"

    def url_for_dividend(self):
        return "https://trade.bosera.com/acctQry/dividendHistory.json"

    def parse_summary_data(self, data):
        """ 汇总页的数据解析规则 """
        for i in data['resultList']:
            if i['fundId'] != self.pk:
                continue
            self.name = i['fundNm']
            self.total_assets = i['endVal']


def main(*funds, detail=False):
    total = 0
    for fund in funds:
        try:
            if hasattr(fund, "funds"):
                for f in fund.funds:
                    x = Xirr(f)
                    print("年化收益率: {:>7.2f}%  市值: {:>7d}  基金代码 {:<7s} {:<30s}".format(x.calc_rate(), int(f.total_assets), f.pk, f.name))
                    if detail:
                        for i in x.trade_list:
                            print("日期: {}  金额: {:>12.2f}  类型: {:\u3000<6}  来源: {}".format(i[0], i[1], i[2], i[3]))
                        print()

            x = Xirr(fund)
            print("年化收益率: {:>7.2f}%  市值: {:>7d}  基金代码 {:<7s} {:<30s}".format(x.calc_rate(), int(fund.total_assets), fund.pk, fund.name))
            if detail:
                for i in x.trade_list:
                    print("日期: {}  金额: {:>12.2f}  类型: {:\u3000<6}  来源: {}".format(i[0], i[1], i[2], i[3]))

            # total += int(fund.total_assets)
            print()
        except Exception as e:
            print(e)


if __name__ == "__main__":
    # RuiyuanFund("007119")
    main(DanjuanPlan("CSI666"))
