# GEE Web App

A browser-based tool for downloading satellite data (precipitation, temperature, vegetation, land cover, and more) from Google Earth Engine for any area you choose.

---

## Before you start

You need two things:

**1. Docker Desktop**
This is the only software you need to install. Download it from [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop) and follow the installer. Once it is installed and running, you are ready.

**2. A Google Earth Engine key file**
A small file (ending in `.json`) that gives the app access to Google Earth Engine. If you do not have one yet, see [Getting a key](#getting-a-key) below.

---

## Starting the app

1. Put the `gee_web_app` folder somewhere on your computer.

2. Open a terminal inside that folder.
   - **Mac:** right-click the folder in Finder → **New Terminal at Folder**
   - **Windows:** open the folder, click the address bar, type `cmd`, press Enter

3. Type the following and press Enter:
   ```
   ./quickstart.sh
   ```
   The first time you do this it will take a few minutes to set up. When it is ready you will see a message with a link, for example:
   ```
   Streamlit UI is ready at http://localhost:8501
   ```

4. Click that link (or paste it into your browser). The app opens.

---

## Stopping the app

When you are done, go back to the terminal and type:
```
./stop.sh
```

Closing the browser tab does not stop the app — you need to run `stop.sh`.

---

## Getting a key

1. Go to [Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts) and select your project.
2. Open or create a service account that has the **Earth Engine** role.
3. Click **Keys → Add Key → Create new key → JSON** and download the file.

The first time you open the app it will ask you to upload this file. After that it remembers it and you will not be asked again.

---

For a full guide on using the app, see the [User Manual](USER_MANUAL.md).
