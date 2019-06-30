#%%
import requests
from credentials import credentials
from apps import apps


class ironsource: 

    def ironsourceAuthentication(self):
        auth_url = 'https://platform.ironsrc.com/partners/publisher/auth'

        print(auth_url)
        auth_headers = {
            'secretkey': credentials['ironsource_secretKey_jc'],
            'refreshToken': credentials['ironsource_refreshToken_jc']
        }
        payload = ''
        authToken = requests.request('GET', auth_url, headers= auth_headers, data=payload)
        print('Authenicated Complete')
        return authToken.text.strip('\"')

    def getMediationData(self , startDate, endDate, appkey, breakdown):

        url = 'https://platform.ironsrc.com/partners/publisher/mediation/applications/v6/stats?startDate='+ startDate +'&endDate='+endDate+'&appKey='+appkey+'&breakdown=' + breakdown

        print(self.ironsourceAuthentication)
        authorizedToken = self.ironsourceAuthentication()
        headers = {
            'Authorization': 'Bearer '+ authorizedToken
        }
        payload = ''
        data = requests.request('GET', url, headers = headers, data = payload)
        print('Data Pulled Complete')

        return data.json()


