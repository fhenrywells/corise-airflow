# if using mac, install via brew

if [[ $1 == 'mac' ]]
then
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"  
    echo '# Set PATH, MANPATH, etc., for Homebrew.' >> /Users/henryweller/.zprofile
    echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> /Users/henryweller/.zprofile
    eval "$(/opt/homebrew/bin/brew shellenv)"
    brew install astro
elif [[ $1 == 'windows' ]]
then
    winget install -e --id Astronomer.Astro   

elif [[ $1 == 'linux' ]]
then
    curl -sSL install.astronomer.io | sudo bash -s
fi

astro dev start
