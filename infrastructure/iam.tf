resource "aws_iam_role" "glue_role" {
  name               = "glue-job-role-tf"
  path               = "/"
  description        = "Provides the required permissions to glue services"
  assume_role_policy = file("./permissions/glue-job-assume-policy-tf.json")
}

resource "aws_iam_policy" "glue_policy" {
  name        = "glue-job-policy-tf"
  path        = "/"
  description = "Provides the required permissions to glue services"
  policy      = file("./permissions/glue-job-policy-tf.json")
}

resource "aws_iam_role_policy_attachment" "glue_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_policy.arn
}