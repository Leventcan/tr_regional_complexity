"""


@author: eguney
         lgultekin

"""
#%% Economic Complexity Ana Tablonun elde edilmesi

from matplotlib.cbook import print_cycles

import pandas as pd
import numpy as np
import feather

from ecomplexity import ecomplexity
from ecomplexity import proximity

import os




data = feather.read_dataframe("province_exports_2013_2020.feather")
world_data = feather.read_dataframe("world_data.feather")

yil=2014    #yıl buradan seçiliyor, kaydederken de yılları değiştirmek lazım

data = data.dropna()
data = data[data.IHRITH == "İHRACAT"]
data = data[(data.YIL==yil)]
data = data.groupby(["YIL", "IL_KODU", "IL_ADI", "SITC", "SITC_ADI"])["DOLAR"].sum().reset_index()
data = data[["YIL", "IL_KODU", "SITC", "DOLAR"]]
data = data.rename(columns ={'YIL': 'year', 'IL_KODU': 'location_code', 
                            "SITC" : "sitc_code", "DOLAR" : "export_value"})




world_data = world_data[(world_data.yr == yil)]
world_data = world_data.groupby(["yr", "rt3ISO", "cmdCode"])["TradeValue"].sum().reset_index()
world_data = world_data.rename(columns ={'yr': 'year', 'rt3ISO': 'location_code', 
                            "cmdCode" : "sitc_code", "TradeValue" : "export_value"})




# Küresel ihracat verisinde bulununan tekil ürün kodları
world_data.columns
unique_products_world = world_data["sitc_code"].unique()
len(unique_products_world)

# İl ihracat verisinde bulunan telik ürün kodları
data.columns
unique_products_province = data["sitc_code"].unique()
len(unique_products_province)

# Küresel ve il bazında ürün kodlarının kesişim kümesi

unique_products = np.intersect1d(unique_products_province, unique_products_world)
len(unique_products)



# Kesişim kümesi ürün kodlarına göre iki verisetinin de filtrelenmesi


world_data = world_data[world_data.sitc_code.isin(unique_products)]
data = data[data.sitc_code.isin(unique_products)]



trade_cols = {'time':'year', 'loc':'location_code', 'prod':'sitc_code', 'val':'export_value'}

# Kompleksite endekslerinin hesaplanması
cdata = ecomplexity(data, trade_cols)
cdata_world = ecomplexity(world_data, trade_cols)

# Küresel ticaret verilerine göre ürün kompleksitesi değerlerinin hesaplanması
pci_values = cdata_world.groupby(["sitc_code", "pci"]).size().reset_index()
pci_values = pci_values[["sitc_code", "pci"]]
pci_values.columns

# Küresel ticaret verilerine göre ürün yakınlık verisinin hesaplanması

prox_world = proximity(world_data, trade_cols)


# İl ihracat verilerine göre yakınlık matrisinin hesaplanması
prox_province = proximity(data, trade_cols)





#İl Verisi için RCA değerlerinin olduğu dataframe
cdata[["location_code", "sitc_code", "mcp"]]


#TODO: Density değerini hesaplayacak bir fonksiyon yazılacakk








# # cdata ve proximity dataframi .csv olarak kayıt etme

# export_excel = cdata.to_csv(r'stic4_cdata_240122.csv', index = None, header=True,encoding='utf-8-sig')

# export_excel_1 = prox_df1.to_csv(r'stic4_proxy_240122.csv', index = None, header=True, encoding='utf-8-sig')


#%% Sektör tanımlarının Eklenmesi

#TODO: Dosya ismi değişecek, string olarak okuma sağlanacak
stic_tanim = pd.read_csv('stic_tanim.csv', sep=',',encoding='windows-1254')
location_name = pd.read_csv('location_name.csv',header=None, sep=',',encoding='windows-1254')

location_name.columns=["location_code","location_name"]

cdata_tanimli=pd.merge(cdata,stic_tanim,on='hs_product_code')
cdata_tanimli=pd.merge(cdata_tanimli,location_name, on="location_code")


cdata_tanimli.columns

cdata=cdata_tanimli[['location_code', "location_name", 'hs_product_code','stic_tanim', 'export_value', 'year', 'diversity',
       'ubiquity', 'mcp', 'eci','coi', 'density','cog','pci', 'rca']]


#%% 
# Öneri Faaliyet Alanları(PCI>ortalama)

il=59

oneriFaaliyetler=cdata[(cdata.year==yil)&(cdata.location_code==il)&(cdata.rca<1)&
                       (cdata.pci>cdata[(cdata.year==yil)&(cdata.location_code==il)].pci.mean())&
                       (cdata.density>cdata[(cdata.year==yil)&(cdata.location_code==il)].density.mean())&
                        (cdata.cog>0)].sort_values(by="density", ascending=False).reset_index(drop=True)

print(yil," yili için ", il, " plaka no'lu ilde density sırasına göre önerilen faaliyetler PCImean \n", oneriFaaliyetler.hs_product_code)  

# Öneri Faaliyet Alanları(PCI>0.25 çeyreklik)

oneriFaaliyetlerPCI25q=cdata[(cdata.year==yil)&(cdata.location_code==il)&(cdata.rca<1)&
                       (cdata.pci>cdata[(cdata.year==yil)&(cdata.location_code==il)].pci.quantile(q=0.25))&
                       (cdata.density>cdata[(cdata.year==yil)&(cdata.location_code==il)].density.mean())&
                        (cdata.cog>0)].sort_values(by="density", ascending=False).reset_index(drop=True)


print(yil," yili için ", il, " plaka no'lu ilde density sırasına göre önerilen faaliyetler PCI25q \n", oneriFaaliyetlerPCI25q.hs_product_code)  

#%% 
#Daha hızlı Cdata içinde tüm yıllar ve tüm iller için önerilenFaaliyetler ve oneriFaaliyetlerPCI25q
#column'u oluşturma

cdata["oneriFaaliyetler"]=""
cdata["oneriFaaliyetlerPCI25q"]=""         
for yil in set(cdata["year"]):
    for il in set(cdata["location_code"]):
        a=cdata[(cdata.year==yil)&(cdata.location_code==il)&(cdata.rca<1)&
                       (cdata.pci>cdata[(cdata.year==yil)&(cdata.location_code==il)].pci.mean())&
                       (cdata.density>cdata[(cdata.year==yil)&(cdata.location_code==il)].density.mean())&
                        (cdata.cog>0)].index.values
        cdata["oneriFaaliyetler"][a]=1
        b=cdata[(cdata.year==yil)&(cdata.location_code==il)&(cdata.rca<1)&
                (cdata.pci>cdata[(cdata.year==yil)&(cdata.location_code==il)].pci.quantile(q=0.25))&
                (cdata.density>cdata[(cdata.year==yil)&(cdata.location_code==il)].density.mean())&
                (cdata.cog>0)].index.values
        cdata["oneriFaaliyetlerPCI25q"][b]=1
cdata["oneriFaaliyetlerPCI25q"][cdata[~(cdata.oneriFaaliyetlerPCI25q==1)].index.values]=0
cdata["oneriFaaliyetler"][cdata[~(cdata.oneriFaaliyetler==1)].index.values]=0


#%% Muhammed Hocanın Sıralaması

cdata1=cdata.sort_values(["year","location_code","density"],ascending=[True,True, False])
cdata1["rank_density"]=cdata1.groupby(["year","location_code"])["density"].rank(method="min",ascending=False)

cdata1=cdata1.sort_values(["year","location_code","cog"],ascending=[True,True, False])
cdata1["rank_cog"]=cdata1.groupby(["year","location_code"])["cog"].rank(method="min",ascending=False)

cdata1=cdata1.sort_values(["year","location_code","pci"],ascending=[True,True, False])
cdata1["rank_pci"]=cdata1.groupby(["year","location_code"])["pci"].rank(method="min",ascending=False)

cdata1["metric_m"]=(cdata1["rank_density"]+cdata1["rank_cog"]+cdata1["rank_pci"])/3
cdata1=cdata1.sort_values(["year","location_code","metric_m"],ascending=[True,True, True])
cdata1["rank_m"]=cdata1.groupby(["year","location_code"])["metric_m"].rank(method="min",ascending=True)
cdata1["rank_m_descending"]=cdata1.groupby(["year","location_code"])["metric_m"].rank(method="min",ascending=False)


#%% 
# Muhammed Hocanın Sıralaması 2- RCA<1, pci>0.25quantile, density>mean, cog>0

cdata2=cdata1.loc[cdata1.oneriFaaliyetlerPCI25q==1]

cdata2=cdata2.sort_values(["year","location_code","density"],ascending=[True,True, False])
cdata2["rank_density1"]=cdata2.groupby(["year","location_code"])["density"].rank(method="min",ascending=False)

cdata2=cdata2.sort_values(["year","location_code","cog"],ascending=[True,True, False])
cdata2["rank_cog1"]=cdata2.groupby(["year","location_code"])["cog"].rank(method="min",ascending=False)

cdata2=cdata2.sort_values(["year","location_code","pci"],ascending=[True,True, False])
cdata2["rank_pci1"]=cdata2.groupby(["year","location_code"])["pci"].rank(method="min",ascending=False)

cdata2["metric_m1"]=(cdata2["rank_density1"]+cdata2["rank_cog1"]+cdata2["rank_pci1"])/3
cdata2=cdata2.sort_values(["year","location_code","metric_m1"],ascending=[True,True, True])
cdata2["rank_m1"]=cdata2.groupby(["year","location_code"])["metric_m1"].rank(method="min",ascending=True)

cdata2m=cdata2[["rank_density1","rank_cog1","rank_pci1","metric_m1","rank_m1"]]

cdata1[["rank_density1","rank_cog1","rank_pci1","metric_m1","rank_m1"]]=""

cdata1["rank_density1"].loc[cdata2m.index.values]=cdata2m["rank_density1"]
cdata1["rank_cog1"].loc[cdata2m.index.values]=cdata2m["rank_cog1"]
cdata1["rank_pci1"].loc[cdata2m.index.values]=cdata2m["rank_pci1"]
cdata1["metric_m1"].loc[cdata2m.index.values]=cdata2m["metric_m1"]
cdata1["rank_m1"].loc[cdata2m.index.values]=cdata2m["rank_m1"]

cdata1["rank_density1"].loc[cdata1.oneriFaaliyetlerPCI25q==0]=0
cdata1["rank_cog1"].loc[cdata1.oneriFaaliyetlerPCI25q==0]=0
cdata1["rank_pci1"].loc[cdata1.oneriFaaliyetlerPCI25q==0]=0
cdata1["metric_m1"].loc[cdata1.oneriFaaliyetlerPCI25q==0]=0
cdata1["rank_m1"].loc[cdata1.oneriFaaliyetlerPCI25q==0]=0

cdata3=cdata2.loc[(cdata2.year==yil)&(cdata.location_code==il)].sort_values(["year","location_code","rank_m1"],ascending=[True,True, True])

#%% Standartlaştırma ve Stratejiler ve Yaklaşımlara göre metriklerin hesaplanması
# df.groupby('indx').transform(lambda x: (x - x.mean()) / x.std())


cdata1["density_std"]=cdata1.groupby(["year","location_code"])["density"].transform(lambda x: (x - x.mean()) / x.std())
cdata1["cog_std"]=cdata1.groupby(["year","location_code"])["cog"].transform(lambda x: (x - x.mean()) / x.std())
cdata1["pci_std"]=cdata1.groupby(["year","location_code"])["pci"].transform(lambda x: (x - x.mean()) / x.std())


cdata1["All_LongJumps_45_35_20"]=(cdata1["density_std"]*0.45)+(cdata1["cog_std"]*0.35)+(cdata1["pci_std"]*0.20) 
cdata1["StrategicBets_BalancedPortfolio_50_35_15"]=(cdata1["density_std"]*0.50)+(cdata1["cog_std"]*0.35)+(cdata1["pci_std"]*0.15)

cdata1["Parsimonious_BalancedPortfolio_55_25_20"]=(cdata1["density_std"]*0.55)+(cdata1["cog_std"]*0.25)+(cdata1["pci_std"]*0.20)

cdata1["LightTouch_BalancedPortfolio_60_20_20"]=(cdata1["density_std"]*0.60)+(cdata1["cog_std"]*0.20)+(cdata1["pci_std"]*0.20)

cdata1["All_LowHangingFruit_60_25_15"]=(cdata1["density_std"]*0.60)+(cdata1["cog_std"]*0.25)+(cdata1["pci_std"]*0.15) 

cdata1=cdata1.sort_index()

cdata1_yil=cdata1[cdata1["year"]==yil]


#%% 
#eci&coi grafiği çizdirme
cdata.columns
cdataUnique=cdata[["location_code","year","eci","coi"]]
cdataUnique=cdataUnique[cdataUnique.year==yil]
cdataUnique=cdataUnique.drop_duplicates(subset=['location_code']) 

cdataUnique.drop("year", axis=1, inplace=True)
location_name = pd.read_csv('location_name.csv',header=None, sep=',')
location_name.columns=["location_code","location_name"]
cdataUnique=pd.merge(cdataUnique,location_name, on="location_code")   

import matplotlib.pyplot as plt
fig, ax = plt.subplots()
ax.scatter(cdataUnique.eci, cdataUnique.coi)

for i, txt in enumerate(cdataUnique.location_name):
    ax.annotate(txt, (cdataUnique.eci.iloc[i], cdataUnique.coi.iloc[i]))

plt.scatter(cdataUnique.eci, cdataUnique.coi)
plt.title(f"eci vs coi grafiği_{yil}")
plt.xlabel("eci")
plt.ylabel("coi")
eci=-np.sort(-cdata1['eci'].unique())    #ecilerin sıralanması
eci_threshold=eci[6]-0.01               #eci eşiği olarak 7. ilin seçilmesi
plt.axvline(eci_threshold)
plt.axhline(0)
plt.text(-1, 2, "Parsimonious", size=20, rotation=0,
         ha="center", va="center",
         bbox=dict(boxstyle="round",
                   ec=(1., 0.5, 0.5),
                   fc=(1., 0.8, 0.8),
                   )
         )
plt.text(-0.5, -2, "Strategic Bets", size=20, rotation=0,
         ha="center", va="center",
         bbox=dict(boxstyle="round",
                   ec=(1., 0.5, 0.5),
                   fc=(1., 0.8, 0.8),
                   )
         )
plt.text(3, 2, "Light Touch", size=20, rotation=0,
         ha="center", va="center",
         bbox=dict(boxstyle="round",
                   ec=(1., 0.5, 0.5),
                   fc=(1., 0.8, 0.8),
                   )
         )
plt.text(2.5, -2, "Technological Frontier", size=20, rotation=0,
         ha="center", va="center",
         bbox=dict(boxstyle="round",
                   ec=(1., 0.5, 0.5),
                   fc=(1., 0.8, 0.8),
                   )
         )
figManager = plt.get_current_fig_manager()
figManager.window.showMaximized()
plt.show()
#%%
eci=-np.sort(-cdata1['eci'].unique())    #ecilerin sıralanması

eci_threshold=eci[6]-0.01               #eci eşiği olarak 7. ilin seçilmesi
coi_threshold=0

cdata1['strateji']=""
cdata1['strateji_metric']=""

for yil in set(cdata1["year"]):
    for il in set(cdata1["location_code"]):
        c=cdata1[(cdata1.year==yil)&(cdata1.location_code==il)&(cdata1.eci<=eci_threshold)&
                       (cdata1.year==yil)&(cdata1.location_code==il)&(cdata1.coi<=coi_threshold)].index.values
        cdata1['strateji'][c]="StrategicBets"
        cdata1['strateji_metric'][c]=cdata1['StrategicBets_BalancedPortfolio_50_35_15'][c]
        d=cdata1[(cdata1.year==yil)&(cdata1.location_code==il)&(cdata1.eci<=eci_threshold)&
                       (cdata1.year==yil)&(cdata1.location_code==il)&(cdata1.coi>coi_threshold)].index.values
        cdata1['strateji'][d]="Parsimonious"
        cdata1['strateji_metric'][d]=cdata1['Parsimonious_BalancedPortfolio_55_25_20'][d]
        e=cdata1[(cdata1.year==yil)&(cdata1.location_code==il)&(cdata1.eci>eci_threshold)&
                       (cdata1.year==yil)&(cdata1.location_code==il)&(cdata1.coi>=coi_threshold)].index.values
        cdata1['strateji'][e]="LightTouch"
        cdata1['strateji_metric'][e]=cdata1['LightTouch_BalancedPortfolio_60_20_20'][e]
        f=cdata1[(cdata1.year==yil)&(cdata1.location_code==il)&(cdata1.eci>eci_threshold)&
                       (cdata1.year==yil)&(cdata1.location_code==il)&(cdata1.coi<coi_threshold)].index.values
        cdata1['strateji'][f]="TechnologicalFrontier" 
        cdata1['strateji_metric'][f]=cdata1['rank_m_descending'][f]
        

cdata2=cdata1.sort_values(["strateji","year","location_code","strateji_metric"],ascending=[True,True,True, False])
cdata4=cdata1[(cdata1.rca<1)].sort_values(["strateji","year","location_code","strateji_metric"],ascending=[True,True,True, False])


cdata5=pd.DataFrame()

for yil in set(cdata4["year"]):
    for il in set(cdata4["location_code"]):
        cdata5=pd.concat([cdata5,cdata4[(cdata4.year==yil)&(cdata4.location_code==il)].iloc[:50,:]])
cdata5.reset_index(drop=True,inplace=True)

#%% İl sırasının düzeltilmesi
cdata6=pd.DataFrame()

for yil in set(cdata4["year"]):
    for il in set(cdata4["location_code"]):
        cdata6=pd.concat([cdata6,cdata4[(cdata4.year==yil)&(cdata4.location_code==il)]])
cdata6.reset_index(drop=True,inplace=True)

cdata6_columnsname=cdata6.rename(columns={"location_code":"plaka_kodu",
                       "location_name":"il_adi",
                       "hs_product_code":"stic_kodu",
                       "export_value":"ihracat",  
                       "year":"yil",
                       })

export_excel6 = cdata6_columnsname.to_csv(r'sonuc/2014_export_cdata6_stic_tum_il_strateji_sirali_eci7nciil_rcakucuk1_240122.csv',index = None, header=True,encoding='utf-8-sig')

#%% Verilerin kolon adları düzeltilmesi-türkçe yapılması

cdata1_columnsname=cdata1.rename(columns={"location_code":"plaka_kodu",
                       "location_name":"il_adi",
                       "hs_product_code":"stic_kodu",
                       "export_value":"ihracat",  
                       "year":"yil",
                       })

cdata2_columnsname=cdata2.rename(columns={"location_code":"plaka_kodu",
                       "location_name":"il_adi",
                       "hs_product_code":"stic_kodu",
                       "export_value":"ihracat",  
                       "year":"yil",
                       })

cdata4_columnsname=cdata4.rename(columns={"location_code":"plaka_kodu",
                       "location_name":"il_adi",
                       "hs_product_code":"stic_kodu",
                       "export_value":"ihracat",  
                       "year":"yil",
                       })

cdata5_columnsname=cdata5.rename(columns={"location_code":"plaka_kodu",
                       "location_name":"il_adi",
                       "hs_product_code":"stic_kodu",
                       "export_value":"ihracat",  
                       "year":"yil",
                       })

#%% Verilerin Kayıt edilmesi

export_excel1 = cdata1_columnsname.to_csv(r'sonuc/2014_export_cdata1_stic_tum_il_strateji_eci7nciil_240122.csv', index = None, header=True,encoding='utf-8-sig')
# export_excel2 = cdata2_columnsname.to_csv(r'sonuc/2014_export_cdata2_stic_tum_il_strateji_sirali_eci7nciil_240122.csv', index = None, header=True,encoding='utf-8-sig')
# export_excel4 = cdata4_columnsname.to_csv(r'sonuc/2014_export_cdata4_stic_tum_il_strateji_sirali_eci7nciil_rcakucuk1_240122.csv', index = None, header=True,encoding='utf-8-sig')
export_excel5 = cdata5_columnsname.to_csv(r'sonuc/2014_export_cdata5_stic_tum_il_strateji_sirali_eci7nciil_rcakucuk1_ilk50_240122.csv',index = None, header=True,encoding='utf-8-sig')
       

# %%
