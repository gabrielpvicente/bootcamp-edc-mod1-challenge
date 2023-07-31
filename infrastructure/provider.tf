provider "aws" {
  region = var.region
}

terraform {
    backend "s3" {
        bucket  = "datalake-gabrielpvicente-352855538020"
        key     = "state-file/edc/module1/terraform.tfstate"
        region  = "us-east-1"
    }
}