#%%  importando as bibliotecas
import pandas as pd
import numpy as np
from datetime import date
import quantstats as qs
import matplotlib.pyplot as plt

#%% tratamento roic

dados=pd.read_csv(r'C:\Users\rique\.spyder-py3\roic.csv',delimiter=';')
dados.columns=dados.columns.str.replace('consolid:sim*','')
dados.columns=dados.columns.str.replace('*','')
dados.columns=dados.columns.str.strip()
dados.Data=pd.to_datetime(dados.Data,dayfirst=True)
dados=dados.replace('-',np.nan)
dados=dados.melt(id_vars='Data')
dados.value=dados.value.str.replace('.','')
dados.value=dados.value.str.replace(',','.')
dados.value=pd.to_numeric(dados.value)
roic=dados.pivot_table(index='Data',columns='variable',values='value')
roic=roic.reset_index()

#%% tratamento negociabilidade

dados=pd.read_csv(r'C:\Users\rique\.spyder-py3\índice_de_negociabilidade.csv',delimiter=';')
dados.columns=dados.columns.str.strip()
dados.Data=pd.to_datetime(dados.Data,dayfirst=True)
dados=dados.replace('-',np.nan)
dados=dados.melt(id_vars='Data')
dados.value=dados.value.str.replace(',','.')
dados.value=pd.to_numeric(dados.value)
negoc=dados.pivot_table(index='Data',columns='variable',values='value')
negoc=negoc.reset_index()

#%% tratamento fechamento

dados=pd.read_csv(r'C:\Users\rique\.spyder-py3\Fechamento.csv',delimiter=';')
dados.columns=dados.columns.str.strip()
dados.Data=pd.to_datetime(dados.Data, dayfirst=True)
dados=dados.replace('-',np.nan)
dados=dados.melt(id_vars='Data')
dados.value=dados.value.str.replace(',','.')
dados.value=pd.to_numeric(dados.value)
fechamento=dados.pivot_table(index='Data',columns='variable',values='value')
fechamento=fechamento.reset_index()


#%% backtest da estratégia com a variação dos lookbacks e rebalanceamento, as tabelas dos resultados são geradas no final

for k in range(1,12):
    
    reb=k
    sens_final=[]
    
    for j in range(3,13):
        
        lookback_m=j
        sens=[]
        
        for i in range(6,19):
            
            lookback_vol=i
            data_inicial=pd.Timestamp(2005,1,1)
            data_final=fechamento['Data'].iloc[-1]
            retorno=pd.DataFrame({'carteira':[],'IBrX':[],'intermediários':[],'perdedores':[]})
           
                             
            while True:
                
                rebalanceamento=data_inicial+pd.DateOffset(months=reb)
                if rebalanceamento<data_final:
                   # ibrx 
                    data_analise0=data_inicial-pd.DateOffset(months=12)
                    ibrx=negoc[(negoc['Data']>=data_analise0)&(negoc['Data']<data_inicial)]
                    ibrx=ibrx.set_index('Data')
                    ibrx=ibrx.sum()
                    ibrx=ibrx.reset_index()
                    ibrx=ibrx.drop(0)
                    ibrx=ibrx.sort_values(by=0,ascending=False)
                    ibrx=list(ibrx['variable'].iloc[:100])
                    
                    # filtrando top 80% do roic: fator quality
                    data_analise1=data_inicial-pd.DateOffset(months=3)
                    data_analise2=data_analise1-pd.DateOffset(months=3)
                    filtro1=roic[(roic['Data']>=data_analise2)&(roic['Data']<data_analise1)]
                    filtro1=filtro1.melt(id_vars='Data')
                    filtro1=filtro1[filtro1['variable'].isin(ibrx)]
                    filtro1=filtro1.pivot_table(index='Data',columns='variable',values='value')
                    filtro1=filtro1.mean()
                    filtro1=filtro1.reset_index()
                    filtro1=filtro1.sort_values(by=0,ascending=False)
                    top_80=int(len(ibrx)*0.8)
                    filtro1=list(filtro1['variable'].iloc[0:top_80])
                    
                   
                    #filtrando top 80% menores volatilidades: fator low vol
                    data_analise3=data_inicial-pd.DateOffset(months=lookback_vol)
                    filtro2=fechamento[(fechamento['Data']>=data_analise3)&(fechamento['Data']<data_inicial)]
                    filtro2=filtro2.melt(id_vars='Data')
                    filtro2=filtro2[filtro2['variable'].isin(filtro1)]
                    filtro2=filtro2.pivot_table(index='Data',columns='variable',values='value')
                    filtro2=filtro2.std()
                    filtro2=filtro2.reset_index()
                    filtro2=filtro2.sort_values(by=0,ascending=True)
                    top80_std=int(len(filtro2)*0.8)
                    filtro2=list(filtro2['variable'].iloc[0:top80_std])
                    
                    #filtrando top 30% retornos(winers): fator momentum
                    data_analise4=data_inicial-pd.DateOffset(months=lookback_m)
                    filtro3=fechamento[(fechamento['Data']>=data_analise4)&(fechamento['Data']<data_inicial)]
                    filtro3=filtro3.melt(id_vars='Data')
                    filtro3=filtro3[filtro3['variable'].isin(filtro2)]
                    filtro3=filtro3.dropna(axis=0,how='any')
                    filtro3=filtro3.pivot_table(index='Data',columns='variable',values='value')
                    filtro3=filtro3.pct_change()
                    filtro3=filtro3.add(1).cumprod().add(-1)
                    filtro3=filtro3.tail(1)
                    filtro3=filtro3.melt()
                    filtro3=filtro3.sort_values(by='value',ascending=False)
                    top30_retornos=int(len(filtro3)*0.3)
                    filtro3=list(filtro3['variable'].iloc[0:top30_retornos])
                    
                    #filtrando top 30% a 70% retornos(Intermediários): fator momentum
                    
                    filtro4=fechamento[(fechamento['Data']>=data_analise4)&(fechamento['Data']<data_inicial)]
                    filtro4=filtro4.melt(id_vars='Data')
                    filtro4=filtro4[filtro4['variable'].isin(filtro2)]
                    filtro4=filtro4.dropna(axis=0,how='any')
                    filtro4=filtro4.pivot_table(index='Data',columns='variable',values='value')
                    filtro4=filtro4.pct_change()
                    filtro4=filtro4.add(1).cumprod().add(-1)
                    filtro4=filtro4.tail(1)
                    filtro4=filtro4.melt()
                    filtro4=filtro4.sort_values(by='value',ascending=False)
                    top70_retornos=int(len(filtro4)*0.7)
                    filtro4=list(filtro4['variable'].iloc[top30_retornos:top70_retornos])
                    
                     #filtrando 70% a 100% dos retornos(Losers): fator momentum
                    
                    filtro5=fechamento[(fechamento['Data']>=data_analise4)&(fechamento['Data']<data_inicial)]
                    filtro5=filtro5.melt(id_vars='Data')
                    filtro5=filtro5[filtro5['variable'].isin(filtro2)]
                    filtro5=filtro5.dropna(axis=0,how='any')
                    filtro5=filtro5.pivot_table(index='Data',columns='variable',values='value')
                    filtro5=filtro5.pct_change()
                    filtro5=filtro5.add(1).cumprod().add(-1)
                    filtro5=filtro5.tail(1)
                    filtro5=filtro5.melt()
                    filtro5=filtro5.sort_values(by='value',ascending=False)
                    top100_retornos=int(len(filtro5)*1)
                    filtro5=list(filtro5['variable'].iloc[top70_retornos:top100_retornos])
                    
                    #calculando o retorno da carteira vencedores
                    
                    close=fechamento[(fechamento['Data']>=data_inicial)&(fechamento['Data']<rebalanceamento)]
                    close=close.melt(id_vars='Data')
                    close=close[close['variable'].isin(filtro3)]
                    close=close.pivot_table(index='Data',columns='variable',values='value')
                    close=close.pct_change()
                    close['retornos']=close.mean(axis=1)
                    close=close.iloc[1:]
                    
                    #calculando o retorno da carteira intermediários
                    
                    close2=fechamento[(fechamento['Data']>=data_inicial)&(fechamento['Data']<rebalanceamento)]
                    close2=close2.melt(id_vars='Data')
                    close2=close2[close2['variable'].isin(filtro4)]
                    close2=close2.pivot_table(index='Data',columns='variable',values='value')
                    close2=close2.pct_change()
                    close2['intermediários']=close2.mean(axis=1)
                    close2=close2.iloc[1:]
                    
                    #calculando o retorno da carteira perdedores
                    
                    close3=fechamento[(fechamento['Data']>=data_inicial)&(fechamento['Data']<rebalanceamento)]
                    close3=close3.melt(id_vars='Data')
                    close3=close3[close3['variable'].isin(filtro5)]
                    close3=close3.pivot_table(index='Data',columns='variable',values='value')
                    close3=close3.pct_change()
                    close3['perdedores']=close3.mean(axis=1)
                    close3=close3.iloc[1:]
                    
                    #calculado o retorno do IBRX
                    close1=fechamento[(fechamento['Data']>=data_inicial)&(fechamento['Data']<rebalanceamento)]
                    close1=close1.melt(id_vars='Data')
                    close1=close1[close1['variable'].isin(ibrx)]
                    close1=close1.pivot_table(index='Data',columns='variable',values='value')
                    close1=close1.pct_change()
                    close1['retornos']=close1.mean(axis=1)
                    close1=close1.iloc[1:]
                                      
                    df=pd.DataFrame({'carteira':close['retornos'],'IBrX':close1['retornos'],'intermediários':close2['intermediários'],'perdedores':close3['perdedores']})
                    retorno=retorno.append(df)                
                   
                    data_inicial=data_inicial+pd.DateOffset(months=reb)
                       
                else:
                    xi=(retorno['carteira'].mean()/retorno['carteira'].std())*252**0.5
                    sens.append(xi)
                    break
# criando listas para juntar tudo em um df    
        if j==3:
            m3=[]
            m3=sens
        elif j==4:
            m4=[]
            m4=sens
        elif j==5:
            m5=[]
            m5=sens
        elif j==6:
            m6=[]
            m6=sens
        elif j==7:
            m7=[]
            m7=sens
        elif j==8:
            m8=[]
            m8=sens
        elif j==9:
            m9=[]
            m9=sens
        elif j==10:
            m10=[]
            m10=sens
        elif j==11:
            m11=[]
            m11=sens
        elif j==12:
            m12=[]
            m12=sens
        else:
            break
    
    #%% tabela das combinações dos lookbacks de momentum(3;12) e low vol(3;18)
    sens_final=[(m3),(m4),(m5),(m6),(m7),(m8),(m9),(m10),(m11),(m12)]
    sens_final=pd.DataFrame(sens_final)
    sens_final.columns=(6,7,8,9,10,11,12,13,14,15,16,17,18)
    sens_final.index=(3,4,5,6,7,8,9,10,11,12)
    
#%% montando um df para cada rebalanceamento entre as combinações de low vol e momentum 
   
    if k==1:
        df_reb1=pd.DataFrame()
        df_reb1=sens_final
    elif k==2:
        df_reb2=pd.DataFrame()
        df_reb2=sens_final
    elif k==3:
        df_reb3=pd.DataFrame()
        df_reb3=sens_final
    elif k==4:
        df_reb4=pd.DataFrame()
        df_reb4=sens_final
    elif k==5:
        df_reb5=pd.DataFrame()
        df_reb5=sens_final
    elif k==6:
        df_reb6=pd.DataFrame()
        df_reb6=sens_final
    elif k==7:
        df_reb7=pd.DataFrame()
        df_reb7=sens_final
    elif k==8:
        df_reb8=pd.DataFrame()
        df_reb8=sens_final
    elif k==9:
        df_reb9=pd.DataFrame()
        df_reb9=sens_final
    elif k==10:
        df_reb10=pd.DataFrame()
        df_reb10=sens_final
    elif k==11:
        df_reb11=pd.DataFrame()
        df_reb11=sens_final
    elif k==12:
        df_reb12=pd.DataFrame()
        df_reb12=sens_final
    else:
        break
        
    
#%% gráfico dos retornos de todas as carteiras
quote=retorno.add(1).cumprod().add(-1)

plt.figure(figsize=(15,5))
plt.plot(quote['carteira'],label='winers-top 19')
plt.plot(quote['intermediários'],label='intermediários- top 19-44')
plt.plot(quote['perdedores'],label='Losers- top 44-64')
plt.plot(quote['IBrX'],label='Benchmark - top 100')
plt.legend(loc='upper left')
plt.title('momentum 8 meses-rebalanceamento mensal')
    
#%%backtest competo com a biblioteca quantstats
qs.reports.full(retorno['mercado'],retorno['IBrX'])


