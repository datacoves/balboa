terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "1.0.3"
    }
  }
}

provider "snowflake" {
  organization_name = "CQBVXGL"
  account_name      = "lda41570"
  user              = "mucio"
  password          = "CKzH9SaLdMG9Snp"
}


