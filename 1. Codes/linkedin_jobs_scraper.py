# -*- coding: utf-8 -*-
"""
Created on Thu Mar 10 11:54:05 2022

@author: danieL zamora
"""
import sys
#Para hacer a la libreria creada (linkedin_utils)
sys.path.append(r"./1. Codes")
from linkedin_utils import LinkedinJobPostings
import sqlite3
import json
#Abrimos el txt con las parámetros de busqueda
with open(r"./job_searching_params.txt",encoding='utf-8') as f:
    job_params = json.load(f)


#=============================== OBTENCIÓN DE DATOS =======================================


#Inicializamos el objeto LinkedinJobPostings
lnk_jobs=LinkedinJobPostings(keyword=job_params["keyword"],
                             location=job_params["location"],
                             n_postings=job_params["n_postings"],
                             headers=job_params["headers"]
                             )

#Instanciamos el método get_postings_df para obtener el dataframe
jobs_df=lnk_jobs.get_postings_df()


#=============================== SUBIDA A LA BBDD =======================================

jobs_df=jobs_df.applymap(str)
con = sqlite3.connect('./2. Database/linkedin_jobpostings.db')
jobs_df.to_sql('JOBPOSTINGS', con, if_exists='append', index=False)
#Eliminamos posibles duplicados
cur = con.cursor()
cur.execute("""
            DELETE FROM JOBPOSTINGS
            WHERE rowid NOT IN 
                (
                SELECT MIN(rowid) 
                FROM JOBPOSTINGS 
                GROUP BY JOBPOSTING
                )
            """)

#Since by default Connector/Python does not autocommit,
#it is important to call the method commit() after every transaction that modifies data for tables 
#that use transactional storage engines.
con.commit()
con.close()
