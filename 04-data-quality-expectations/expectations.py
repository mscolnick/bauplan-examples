"""

This script collects bauplan expectations, that is statistical / quality checks that run
against Bauplan models to ensure the data is correct and avoid wasteful computation
or (even worse) non-compliant data artifacts.

This example showcases how you can use the standard expectations provided by bauplan to test your
data in the most efficient way possible.

Note that collecting all expectations in a single file is not required, but we find it useful
to keep the pipeline code clean and separate from the expectations code.

"""

import bauplan
# import the standard expectations from the library to use them in the functions below
from bauplan.standard_expectations import expect_column_no_nulls


# expectations are identified by a special decorator
@bauplan.expectation()
# this is the usual bauplan.python decorator where we can express all the dependencies we need to run the function
@bauplan.python('3.11')
def test_null_values_on_scene_datetime(
        data=bauplan.Model(
            # as input, we declare the Bauplan model that we want to check
            'normalized_taxi_trips',
        )
):
    """

    As we are calculating the difference between request_datetime and on_scene_datetime
    we want toe make sure that on_scene_datetime has no null values.

    """

    # just return the result of the standard expectation (True if passed), passing to it
    # the input data, the column name to check, and the reference value
    # here is where we declare the columns we want to check
    column_to_check = 'on_scene_datetime'
    _is_expectation_correct = expect_column_no_nulls(data, column_to_check)

    # assert the result of the test. In this way, the pipeline will stop running if the expectation tests fails
    # in this way we can prevent data quality issues to become part of our production environment set up alerts.
    assert _is_expectation_correct, f"expectation test failed: we expected {column_to_check} to have no null values"

    # print the result of the test. In this way, the pipeline will not stop even if the expectation tests fails
    # in case of failure we are simply printing out the result of the test.
    # Comment the assert statement above and de-comment the code below to try.
    # if _is_expectation_correct:
    #     print('expectation test passed with flying colors')
    # else:
    #     print(f"expectation test failed! {column_to_check} has null values in it.")

    return _is_expectation_correct  # return a boolean
