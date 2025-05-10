from dagster import asset, AssetExecutionContext

@asset
def my_first_asset(context: AssetExecutionContext):
    """
    This is the asset Description
    """
    print("some print statement")
    context.log.info("This is an info log message")
    return [1,2,3]
