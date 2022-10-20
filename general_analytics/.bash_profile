# format your terminal to make prettier
export PS1="\[\033[1;34m\]\@ \[\033[1;32m\]\w\[\033[0m\] :"
export PATH="/usr/local/bin:${PATH}"
export EDITOR='subl -w'
alias subl='sublime'
# VM Alias
alias vm="ssh pdavidoff.vm.dev.etsycloud.com"


# Setting PATH for Python 3.9
# The original version is saved in .bash_profile.pysave
PATH="/Library/Frameworks/Python.framework/Versions/3.9/bin:${PATH}"
export PATH

# The next line updates PATH for the Google Cloud SDK.
if [ -f '/Users/pdavidoff/Documents/gcloud/google-cloud-sdk/path.bash.inc' ]; then . '/Users/pdavidoff/Documents/gcloud/google-cloud-sdk/path.bash.inc'; fi

# The next line enables shell command completion for gcloud.
if [ -f '/Users/pdavidoff/Documents/gcloud/google-cloud-sdk/completion.bash.inc' ]; then . '/Users/pdavidoff/Documents/gcloud/google-cloud-sdk/completion.bash.inc'; fi
