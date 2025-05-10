import subprocess

def authenticate_ldap_user(email, password):
    base_dn = "dc=smi,dc=com"
    ldap_url = "ldap://localhost:389"
    bind_dn = f"mail={email},ou=users,{base_dn}"

    try:
        result = subprocess.run(
            ["ldapwhoami", "-x", "-D", bind_dn, "-w", password, "-H", ldap_url],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True
        )
        print("Authentication succeeded:", result.stdout.strip())
        return True
    except subprocess.CalledProcessError as e:
        print("Authentication failed:", e.stderr.strip())
        return False

# Example usage
if __name__ == "__main__":
    email = "dev-smi@gmail.com"
    password = "123456789"
    authenticate_ldap_user(email, password)
