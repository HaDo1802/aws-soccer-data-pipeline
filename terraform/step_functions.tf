# Imported Step Functions resources. This file is written to match the existing
# AWS state machine and its dedicated execution role.

# Lookup for the existing scrape-teams-league Lambda, which is part of the
# state machine but is not currently declared in lambdas.tf.
data "aws_lambda_function" "scrape_teams_league" {
  function_name = "scrape-teams-league"
}

# Trust policy for the Step Functions execution role.
data "aws_iam_policy_document" "step_functions_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

# Dedicated role used by the state machine. This is separate from dev_lamda.
resource "aws_iam_role" "step_functions_exec" {
  name               = "StepFunctions-Transfermarkt-Data-Pipeline-role-0pm7ihk4w"
  path               = "/service-role/" # Match the existing AWS IAM role path to avoid replacement.
  assume_role_policy = data.aws_iam_policy_document.step_functions_assume_role.json
}

# Inline policy granting Step Functions permission to invoke every Lambda used
# by the workflow. This includes scrape-teams-league plus the five Lambdas
# already managed in lambdas.tf.
data "aws_iam_policy_document" "step_functions_invoke_lambdas" {
  statement {
    actions = ["lambda:InvokeFunction"]

    resources = [
      aws_lambda_function.scrape_roster.arn,
      "${aws_lambda_function.scrape_roster.arn}:*",
      aws_lambda_function.scrape_players.arn,
      "${aws_lambda_function.scrape_players.arn}:*",
      aws_lambda_function.combine_player_json.arn,
      "${aws_lambda_function.combine_player_json.arn}:*",
      aws_lambda_function.clean_player_stats.arn,
      "${aws_lambda_function.clean_player_stats.arn}:*",
      aws_lambda_function.snowflake_ingest.arn,
      "${aws_lambda_function.snowflake_ingest.arn}:*",
      data.aws_lambda_function.scrape_teams_league.arn,
      "${data.aws_lambda_function.scrape_teams_league.arn}:*",
    ]
  }
}

resource "aws_iam_role_policy" "step_functions_invoke_lambdas" {
  name   = "StepFunctionsInvokeLambdas"
  role   = aws_iam_role.step_functions_exec.id
  policy = data.aws_iam_policy_document.step_functions_invoke_lambdas.json
}

# State machine definition loaded from a JSON template so the Lambda ARNs stay
# dynamic and account-safe.
resource "aws_sfn_state_machine" "pipeline" {
  name     = "Transfermarkt-Data-Pipeline"
  role_arn = aws_iam_role.step_functions_exec.arn
  type     = "STANDARD"

  definition = templatefile("${path.module}/step_functions/pipeline.json", {
    scrape_teams_league_arn = "${data.aws_lambda_function.scrape_teams_league.arn}:$LATEST"
    scrape_roster_arn       = "${aws_lambda_function.scrape_roster.arn}:$LATEST"
    scrape_players_arn      = "${aws_lambda_function.scrape_players.arn}:$LATEST"
    combine_player_json_arn = "${aws_lambda_function.combine_player_json.arn}:$LATEST"
    clean_player_stats_arn  = "${aws_lambda_function.clean_player_stats.arn}:$LATEST"
    snowflake_ingest_arn    = "${aws_lambda_function.snowflake_ingest.arn}:$LATEST"
  })
}

output "step_functions_pipeline_name" {
  description = "Name of the Transfermarkt Step Functions state machine."
  value       = aws_sfn_state_machine.pipeline.name
}

output "step_functions_pipeline_arn" {
  description = "ARN of the Transfermarkt Step Functions state machine."
  value       = aws_sfn_state_machine.pipeline.arn
}
