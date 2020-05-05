# Update requirements.txt 
`pip freeze > $target_requirements`

# Active environments
`source .env/bin/activate`

# Check Syntax 
`reset; cdk synth`

# Deploy
`reset && zaws login search-test && cdk deploy`
