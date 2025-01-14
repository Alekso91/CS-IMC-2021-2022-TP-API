provider "azurerm" {
    features {}
}

resource "azurerm_resource_group" "rg" {
    name     = "ja-api-rg"
    location = "France Central"
    tags = {
       tpapi = "1"
    }
}

resource "azurerm_storage_account" "storage" {
    name                     = "ja-apisto"
    resource_group_name      = azurerm_resource_group.rg.name
    location                 = azurerm_resource_group.rg.location
    account_tier             = "Standard"
    account_replication_type = "LRS"
}

resource "azurerm_app_service_plan" "serviceplan" {
    name                = "ja-api-service-plan"
    location            = azurerm_resource_group.rg.location
    resource_group_name = azurerm_resource_group.rg.name
    kind                = "elastic"
    reserved            = "true"
    sku {
      tier = "Dynamic"
      size = "Y1"
    }
}

resource "azurerm_function_app" "functionapp" {
    name                       = "ja-api-fa"
    location                   = azurerm_resource_group.rg.location
    resource_group_name        = azurerm_resource_group.rg.name
    app_service_plan_id        = azurerm_app_service_plan.serviceplan.id
    storage_account_name       = azurerm_storage_account.storage.name
    storage_account_access_key = azurerm_storage_account.storage.primary_access_key
    os_type                    = "linux"
    version                    = "~3"
    app_settings = {
        "FUNCTIONS_WORKER_RUNTIME" = "python",
        "TPBDD_SERVER" = "tpbdd-movies-sqls.database.windows.net",
        "TPBDD_DB" = "tp2bdd-movies-sql",
        "TPBDD_USERNAME" = "sqladmin",
        "TPBDD_PASSWORD" = ".YBEpru2GDPdrET6eU4u",
        "TPBDD_NEO4J_SERVER" = "bolt://34.227.171.232:7687",
        "TPBDD_NEO4J_USER" = "neo4j",
        "TPBDD_NEO4J_PASSWORD" = "spot-cotton-crewmembers"
    }
}