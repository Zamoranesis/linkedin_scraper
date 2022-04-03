# Linkedin JobPosting Scraper

Pasos para obtener las ofertas de trabajo de Linkedin:

- 1.Obtener "li_at":
    - 1.1 Navegar hasta www.linkedin.com, loggearse y abrir la consola del desarrollador (Ctrl-Shift-I).
    - 1.2 Ir a Application>Cookies (Storage Section)
    - 1.3 Hacer click en el dropdown de la sección de Cookies the Cookies y seleccionar la opción de www.linkedin.com
    - 1.4 Encontrar y copiar el valor del campo "li_at"

- 2.Cambiar el valor del parámetro "li_at" dentro del fichero "job_searching_params.txt"


- 3.Doble click en el archivo batch "jobposting_scraper_executable.bat" para que se ejecute en la linea de comandos
