

# .creds file should look like this 
# export CLIENT_TOKEN="LOTS OF NUMBERS THAT MAKE A TOKEN GO HERE"

git pull
source ~/.ssh/loggerbot.discord.creds
pipenv run python ./Bot.py --env prod "$@"
