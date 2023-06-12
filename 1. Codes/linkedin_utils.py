# -*- coding: utf-8 -*-
"""
Created on Thu Mar 10 10:38:42 2022

@author: daniel zamora
"""

import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime

import pickle


class LinkedinJobPostings():
    
    def __init__(self, keyword : str , location : str, n_postings : int ,headers : dict):
        self.keyword=keyword
        self.location=location
        self.n_postings=n_postings
        self.headers=headers 
        """
        En headers, importantes los parámetros csrf-token y cookie(JSESSIONID , li_at)
        Fuente:https://es.linkedin.com/legal/l/cookie-table?
        JSESSIONID: Se utiliza para obtener protección frente a la vulnerabilidad Cross Site Request Forgery (CSRF)
        li_at: Se utiliza para autenticar a miembros y clientes de API 
        """
        
    def iferror(self, success, failure, *exceptions):
        """
        Función auxiliar en caso de que no exista un determinado campo y de error.
        
        :sucess: resultado en caso de que no dé error
        :failure: resultado en caso de error
        """
        try:
            return success()
        except exceptions or Exception:
            return failure
    
    def get_postings(self):
        """
        Obtiene un array con todos los indicadores de cada una de las ofertas de trabajo en formato string
                  (Ej: ['2951349599', '2943259658',....])
        
        :return: np.array()
        """

        postings_all=[]
        for i in np.arange(0,self.n_postings,50):
          #Obtención del numero de jobpostings
          url = ("https://www.linkedin.com/voyager/api/search/hits?"
                "count=50" #No deja más de 50
                f"&filters=List(locationFallback-%3E{self.location},resultType-%3EJOBS)"
                f"&keywords={self.keyword}"
                f"&start={str(i)}"
                "&q=jserpFilters")
          response = requests.get(url, headers=self.headers)
          if response.status_code==200:
            try:
              resp_json=response.json()["elements"]
              posting=[y["com.linkedin.voyager.search.SearchJobJserp"]["jobPosting"].split(":")[-1] for y in [x["hitInfo"] for x in resp_json]]
              postings_all=np.r_[postings_all,posting] #Concatenamos al array existente
              time.sleep(np.random.uniform(2,3))
            except:
              i-=50
              time.sleep(np.random.uniform(2,3))
              continue
          else:
            i-=50
            time.sleep(np.random.uniform(2,3))
        if len(postings_all)==0: "Falló la obtención de jobpostings"
        return postings_all 
    
    def get_job_postings_info(self,postings):
        """
        Devuelve un dataframe con toda la información relativa a cada una de las ofertas de trabajo (a excepción de la compañía)
        
        :postings: Array con toda los indicadores de cada una de las ofertas de trabajo en formato string
                  (Ej: ['2951349599', '2943259658',....])

        :return: pd.DataFrame()
        """
        
        df=pd.DataFrame(columns=["createdAt","expireAt","jobposting","company_urn",'job_title', "applies",'views','formattedIndustries',
                                  'formattedJobFunctions','formattedLocation','formattedExperienceLevel',
                                  "text","smartSnippets","employmentStatus",'workRemoteAllowed',"country",
                                  'inferredBenefits',"applyMethod"])
        for i,posting in enumerate(postings):
            if i % 100==0: print(f"Recabando información del jobposting {i}")
            url = f"https://www.linkedin.com/voyager/api/jobs/jobPostings/{posting}"
            response = requests.get(url, headers=self.headers)
            resp_json=response.json()
            #Obtención de campos que nos interesan
            #Utilizamos la función iferror porque puede que no existan esos determinados campos en un job-posting concreto
            if response.status_code==200:
                try:
                    fields_dict={
                            "createdAt":datetime.fromtimestamp(resp_json["createdAt"]/1000),
                            "expireAt":datetime.fromtimestamp(resp_json["expireAt"]/1000),
                            "jobposting":"https://www.linkedin.com/jobs/view/" + resp_json["applyingInfo"]["entityUrn"].split(":")[-1],
                            "company_urn":resp_json['companyDetails']['com.linkedin.voyager.jobs.JobPostingCompany']['company'].split(":")[-1],
                            "job_title":resp_json['title'],
                            "applies":self.iferror(lambda:resp_json["applies"],np.nan),
                            "views":self.iferror(lambda:resp_json['views'],np.nan),
                            "formattedIndustries":self.iferror(lambda: resp_json['formattedIndustries'][0], np.nan),
                            "formattedJobFunctions":self.iferror(lambda:resp_json['formattedJobFunctions'][0], np.nan),
                            "formattedLocation":self.iferror(lambda:resp_json['formattedLocation'], np.nan),
                            "formattedExperienceLevel":self.iferror(lambda:resp_json['formattedExperienceLevel'], np.nan),
                            "text":resp_json['description']["text"],
                            "smartSnippets":resp_json["smartSnippets"][0],
                            "employmentStatus":self.iferror(lambda:resp_json["employmentStatus"].split(":")[-1],np.nan),
                            "workRemoteAllowed":self.iferror(lambda:resp_json['workRemoteAllowed'],np.nan),
                            "country":resp_json["country"].split(":")[-1],
                            "inferredBenefits":self.iferror(lambda:resp_json['inferredBenefits'],np.nan),
                            "applyMethod":resp_json["applyMethod"]
                            }
                    
                    # Iteramos en el diccionario convirtiendo cada valor en una serie de pandas
                    # para poder convertir el diccionario en un dataframe y concatenarlo
                    for k in fields_dict.keys():
                        fields_dict[k] = pd.Series(fields_dict[k])
                    
                    # Concatenamos los dataframes
                    df = pd.concat([df, pd.DataFrame(fields_dict)], axis=0, ignore_index=True)
    
                    #Para evitar Timeout Exception Error
                    time.sleep(np.random.uniform(2,3))
                except KeyError as error:
                    print(error)
                    continue
        if df.shape[0]==0: print("Falló la creación del dataframe de jobpostings")
        print("Finalizada la recogida de información de los jobpostings")
        return df
  
    
    def get_company_info(self, postings_df):
        """
        Devuelve un dataframe con toda la información de la compañia para cada una de las ofertas de trabajo
    
        :postings_df: Dataframe con toda la información relativa a las ofertas de trabajo. Es necesario que contenga
                      la columna 'company_urn'
                      
        :return: pd.DataFrame()
        """

        df=pd.DataFrame(columns=["company_urn","company_name","company_headquarters","n_employees",
                                  'company_type','company_description','company_industries',
                                  'company_specialties','company_website'
                                   ])
        for i,urn in enumerate(postings_df["company_urn"].unique()):
            if i % 100==0: print(f"Recabando información de la compañia {i}")
            url="https://www.linkedin.com/voyager/api/entities/companies/"+str(urn)
            response=requests.get(url, headers=self.headers)
            resp_json=response.json()
            #Get fields
            fields_dict={
                        "company_urn":urn,
                        "company_name":self.iferror(lambda:resp_json["basicCompanyInfo"]["miniCompany"]["name"],np.nan),
                        "company_headquarters":self.iferror(lambda:resp_json["basicCompanyInfo"]["headquarters"],np.nan),
                        "n_employees":self.iferror(lambda:resp_json["employeeCountRange"],np.nan),
                        'company_type':self.iferror(lambda:resp_json["companyType"], np.nan),
                        'company_description':self.iferror(lambda:resp_json["description"],np.nan),
                        'company_industries':self.iferror(lambda:resp_json["industries"], np.nan),
                        'company_specialties':self.iferror(lambda:resp_json["specialties"][0], np.nan),
                        'company_website': self.iferror(lambda:resp_json["websiteUrl"] ,np.nan),               
                         }
            # Iteramos para convertir los valores del diccionario en pd.Series
            # y poder crear un dataframe que concatenar posteriormente
            for k in fields_dict.keys():
                        fields_dict[k] = pd.Series(fields_dict[k])
                    
         
            # Concatenamos el dataframe
            df = pd.concat([df, pd.DataFrame(fields_dict)], axis=0, ignore_index=True)
            #Para evitar Timeout Exception Error
            time.sleep(np.random.uniform(2,3))
        
        print("Finalizada la recogida de información de las empresas")
        return df
  
    
    def get_postings_df(self):
        """
        Devuelve un dataframe con toda la información acerca de la oferta de trabajo y la compañía.
        
        :return: pd.DataFrame()
        """
        
        posting=self.get_postings()
        postings_df=self.get_job_postings_info(posting)
        company_df=self.get_company_info(postings_df)
        
        df=postings_df.merge(company_df, on="company_urn",how="left")
        #Ordenamos las columnas
        df=df[["createdAt","expireAt","company_name",
                 "job_title","jobposting","applies","views",
                 "company_urn","formattedIndustries",
                 "formattedJobFunctions","formattedLocation",
                 "formattedExperienceLevel","text","smartSnippets",
                 "employmentStatus","workRemoteAllowed",
                 "country","inferredBenefits","applyMethod",
                 'company_headquarters', 'n_employees',
                 'company_type','company_description',
                 'company_industries','company_specialties','company_website'
                ]]
        #Formateo final
        df['applies']=df['applies'].fillna(0).astype(int)
        df['views']=df['views'].fillna(0).astype(int)

        return df
    
        