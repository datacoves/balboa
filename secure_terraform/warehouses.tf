resource "snowflake_warehouse" "small_warehouse" {
  name                = "SMALL_WAREHOUSE"
  auto_suspend        = 60
  auto_resume         = false
  initially_suspended = true
}

locals {
  warehouses     = yamldecode(file("${path.module}/warehouses.yml"))
  warehouses_map = merge([for wh in local.warehouses : wh]...)

}

resource "snowflake_warehouse" "warehouses" {
  for_each            = merge([for wh in local.warehouses : wh]...)
  name                = each.key
  warehouse_size      = each.value.size
  auto_suspend        = each.value.auto_suspend
  auto_resume         = each.value.auto_resume
  initially_suspended = each.value.initially_suspended
}
