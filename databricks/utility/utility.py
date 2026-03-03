# Databricks notebook source
storage = "retaileromnichannelgen2"
container = "retailer"

tenant_id = dbutils.secrets.get("retailer_scope", "retail-tenant-id")
client_id = dbutils.secrets.get("retailer_scope", "retail-client-id")
client_secret = dbutils.secrets.get("retailer_scope", "retail-client-secret")

configs = {
  f"fs.azure.account.auth.type.{storage}.dfs.core.windows.net": "OAuth",
  f"fs.azure.account.oauth.provider.type.{storage}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  f"fs.azure.account.oauth2.client.id.{storage}.dfs.core.windows.net": client_id,
  f"fs.azure.account.oauth2.client.secret.{storage}.dfs.core.windows.net": client_secret,
  f"fs.azure.account.oauth2.client.endpoint.{storage}.dfs.core.windows.net": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

for k, v in configs.items():
    spark.conf.set(k, v)
