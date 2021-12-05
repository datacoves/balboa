{% docs __overview__ %}
# Datacoves Demo
This project is used as a demonstration of an end-to-end data flow utilizing DataOps best practices and automate processes that:
* Transform data to make it analytics read
* Enforce governance rules
* Capture data lineage
* Capture documentation
* Perform data testing

![](https://www.dataops.live/hubfs/DataOps-Infinty-Blue-1.png)
{% enddocs %}



{% docs __dbt_utils__ %}
# Utility macros
We use this suite of utility macros in our transformations.
{% enddocs %}

{% docs __dbt_expectations__ %}
# Test macros provided by dbt expecations
We use this package to add more advanced tests to our models.
For more information, visit 
<a href="https://github.com/calogica/dbt-expectations" target="_blank">the dbt-expecations site.</a>
{% enddocs %}

{% docs __dbt_date__ %}
# Utility macros used by dbt_utils
We use this suite of utility macros in our transformations.
{% enddocs %}