# airflow_hydraulic_system_monitor

This is a small project for a DAG that saves data in a database and sends an email depending on the temperature of a fake hydraulic system data.

## The GIF above shows how the DAG works:
![Alt Text](url_do_seu_gif)


### To execute this code on your computer, you will need to follow the steps outlined below:
* Ensure that Docker, Python, and an IDE are installed on your system. [Texto do Link](URL_do_Link)
* Create a folder (for example, "airflow").
* Copy the .env and .yaml files contained in this repository to the created folder.
* Open the terminal in the directory you created.
* Execute the command: docker-compose up -d (wait a few seconds for Airflow to become ready).
* Add the hydraulicsystem.py to the "dags" folder and the hydraulicsystemgenerator.py to the "data" folder.
* In the file hydraulicsystem.py, replace instances of 'your_email' with your actual email address.
* In docker-composer.yaml, in the email configuration section, add your email address and your email application password (locate "app password" in your Gmail settings and create one).
* In the AIRFLOW interface, navigate to "Admin" -> "Variables" and create the variables as shown in the image above.
    ![Texto Alternativo](URL_da_Imagem)
* Again in the AIRFLOW interface, go to "Admin" -> "Connections" and create the connections as shown in the image above.
    ![Texto Alternativo](URL_da_Imagem)
* Finally, activate your DAG as shown in the image above, execute hydraulicsystemgenerator.py, and observe the DAG in action.
    ![Texto Alternativo](URL_da_Imagem)

Feel free to contact me via LinkedIn.[Texto do Link](URL_do_Link)