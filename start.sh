# Install brew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"  

echo '# Set PATH, MANPATH, etc., for Homebrew.' >> /Users/henryweller/.zprofile
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> /Users/henryweller/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"


brew install astro

astro dev start
