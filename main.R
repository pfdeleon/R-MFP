library(data.table)
library(stringr)
library(dplyr)
library(doFuture)
library(doRNG)
library(arrow)
library(readxl)

rm(list = ls())

#setwd("/Users/pfdeleon/OneDrive - LABORATORIOS PISA S.A. DE C.V/Ciencia De Datos/Proyectos/11 Mercado Farmaceutico Privado (MFP)/")

#carpeta_exportados <- "//pluton/PRIVADO/ADM/InteligenciacomercialVM/Tablas Power BI/11 Mercado Farmaceutico Privado (MFP)/Productivo"

#write.csv(laboratorios,carpeta_exportados %>% paste0("Productivo/Laboratorios.csv"), fileEncoding = "UTF-8", row.names =  FALSE)


# Enlaces -----------------------------------------------------------------

carpeta_exportados <- "c:/Users/pfdeleon/Desktop/Temporal/"

carpeta_mfp <-
  getwd() %>%
  dirname() %>%
  dirname() %>%
  paste0("/Datos/MFP")

archivos <-
  list.files(
    carpeta_mfp,
    pattern = "csv",
    recursive = TRUE,
    full.names = TRUE
  )

laboratorios <-
  read_xlsx("Referencia/Laboratorios.xlsx") %>%
  as.data.table() %>%
  select(-Comentario)

estados <-
  read_xlsx("Referencia/Estados.xlsx") %>%
  as.data.table()

presentaciones <-
  read_xlsx("Referencia/Productos.xlsx") %>%
  as.data.table()

productos <-
  # Selecciona solamente archivos de Catálogos Productos
  grep("CATALOGO", archivos, value = TRUE) %>%
  lapply(function(x) {
    # Trae la fecha en nombre del archivo
    fecha <-
      str_extract(x, "\\d{4}-\\d{2}") %>%
      paste0("-01") %>%
      as.IDate()
    # Leer archivo y generar columna de Periodo
    fread(x, encoding = "UTF-8") %>%
      mutate(Periodo = fecha,
             `Presentacion CveLab` = str_squish(`Presentacion CveLab`))
  }) %>%
  # Juntar todas las tablas
  rbindlist(use.names = TRUE, fill = TRUE) %>%
  # Quitar columnas no necesarias
  select(-`Punto de Venta`) %>%
  # Limpiar tabla de filas blancas
  na_if("") %>%
  filter(!is.na(Presentacion)) %>%
  # Quitar duplicados
  unique() %>%
  # Homologar Corporacion
  merge(laboratorios,
        by = c("Laboratorio", "Corporacion"),
        all.x = TRUE)

# Exportar laboratorios nuevos
productos[is.na(`Corporacion Homologado`), .(Laboratorio, Corporacion)] %>%
  unique() %>%
  select(Laboratorio, Corporacion) %>%
  write.csv(
    carpeta_exportados %>% paste0("Pendientes/Laboratorios.csv"),
    fileEncoding = "UTF-8",
    row.names = FALSE
  )

# Preparar Presentacion CveLab para cruce de ID_Prod
# Es necesario todas las Presentacion CveLab, por tanto la extracción este paso.
# Paso siguiente: quitar algunas para dejar la más reciente
homologacion_presentacion <-
  productos[, .(`Presentacion CveLab`, Presentacion, `Corporacion Homologado`)] %>%
  unique()

##Traer último laboratorio (Estos productos pueden cambiar de compañía distribuidora. Como estándar se asigna el producto a la distribuidora más reciente)
eticos_otc <-
  filter(productos, Genero %in% c("Etico", "O.T.C.")) %>%
  arrange(Presentacion, desc(Periodo)) %>%
  group_by(Presentacion) %>%
  slice_head() %>%
  as.data.table()

gi_gp <-
  filter(productos,!Genero %in% c("Etico", "O.T.C.")) %>%
  arrange(Presentacion, `Corporacion Homologado`, desc(Periodo)) %>%
  group_by(Presentacion, `Corporacion Homologado`) %>%
  slice_head() %>%
  as.data.table()

productos <-
  bind_rows(eticos_otc, gi_gp) %>%
  select(-Periodo) %>%
  unique() %>%
  mutate(ID_Prod = row_number()) %>%
  mutate(Genero = fifelse(Genero == "G.I.", "G.P.", Genero))

# Exportar dimensión de Productos -----------------------------------------
select(productos, -`Corporacion Homologado`, -`Presentacion CveLab`) %>%
  write.csv(
    carpeta_exportados %>% paste0("Productivo/Productos.csv"),
    fileEncoding = "UTF-8",
    row.names = FALSE
  )

# Asignar ID_Prod a Presentacion CveLab
homologacion_presentacion <-
  merge(
    homologacion_presentacion,
    productos[, .(Presentacion, `Corporacion Homologado`, ID_Prod)],
    by = c("Presentacion", "Corporacion Homologado"),
    all.x = TRUE
  ) %>%
  merge(
    productos[, .(Presentacion, `Corporacion Homologado`, ID_Prod)],
    by = c("Presentacion"),
    all.x = TRUE,
    allow.cartesian = TRUE
  ) %>%
  mutate(ID_Prod = coalesce(ID_Prod.x, ID_Prod.y)) %>%
  select(`Presentacion CveLab`, ID_Prod) %>%
  unique()

# Tabla de hechos ---------------------------------------------------------

# Traer tabla de hechos
registerDoFuture()
plan(multisession)

tabla_hechos <-
  # Selecciona solamente archivos de Catálogos Productos
  grep("CATALOGO", archivos, invert = TRUE, value = TRUE) %>%
  foreach (archivo = .) %dorng% {
    # Trae la fecha en nombre del archivo
    fecha <-
      str_extract(archivo, "\\d{4}-\\d{2}") %>%
      paste0("-01") %>%
      as.IDate()
    # Leer archivo y generar columna de Periodo
    fread(archivo,
          encoding = "UTF-8",
          drop = c("Fecha", "Punto de Venta")) %>%
      mutate(Periodo = fecha,
             `Presentacion CveLab` = str_squish(`Presentacion CveLab`)) %>%
      mutate_at(c("MTH Unidades", "MTH Pesos"), function(x) {
        str_replace_all(x, ",", "") %>%
          as.numeric()
      }) %>%
      merge(presentaciones,
            by.x = "Presentacion CveLab",
            by.y = "Presentacion CveLab (original)",
            all.x = TRUE) %>%
      mutate(
        `Presentacion CveLab` = fifelse(
          is.na(`Presentacion CveLab (correcto)`),
          `Presentacion CveLab`,
          `Presentacion CveLab (correcto)`
        )
      ) %>%
      select(-`Presentacion CveLab (correcto)`) %>%
      filter(!is.na(`MTH Unidades`))
  } %>%
  # Juntar todas las tablas
  rbindlist(use.names = TRUE, fill = TRUE)

plan(sequential)

# Dimensión de Estados
canales <-
  tabla_hechos[, .(Canal)] %>%
  unique() #%>%
  #mutate(ID_Canal = row_number())

estados <-
  tabla_hechos[, .(Estado)] %>%
  unique() %>%
  mutate(ID_Estado = row_number()) %>%
  merge(estados, by = "Estado")

# Exportar Dimensiones ----------------------------------------------------
write.csv(canales,
          "Exportados/Canales.csv",
          fileEncoding = "UTF-8",
          row.names = FALSE)
select(estados, ID_Estado, `Estado ISO`) %>%
  rename(Estado = `Estado ISO`) %>%
  write.csv("Exportados/Estados.csv",
            fileEncoding = "UTF-8",
            row.names = FALSE)

resultado <-
  merge(tabla_hechos,
        homologacion_presentacion,
        by = "Presentacion CveLab",
        all.x = TRUE) %>%
  merge(canales, by = "Canal") %>%
  merge(estados[, .(Estado, ID_Estado)], by = "Estado")

#Productos Pendientes
resultado[is.na(ID_Prod), .(`Presentacion CveLab`)] %>%
  unique() %>%
  write.csv("Pendientes/Productos.csv",
            fileEncoding = "UTF-8",
            row.names = FALSE)

# Exportar Hechos ---------------------------------------------------------
select(resultado,
       ID_Prod,
       ID_Canal,
       ID_Estado,
       Periodo,
       `MTH Unidades`,
       `MTH Pesos`) %>%
  filter(!is.na(ID_Prod)) %>%
  rename(Uni = `MTH Unidades`, Val = `MTH Pesos`) %>%
  write_parquet("Exportados/mfp.parquet")