use anyhow::Result;
use clap::{Parser, Subcommand};
use tonic::transport::Channel;
use tracing_subscriber::EnvFilter;

use gitstratum_proto::auth_service_client::AuthServiceClient;
use gitstratum_proto::{
    AddSshKeyRequest, CreateTokenRequest, CreateUserRequest, SetPermissionRequest,
};

#[derive(Parser, Debug)]
#[command(name = "gitstratum-admin")]
#[command(about = "GitStratum Admin CLI - User and permission management")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:9003")]
    auth_addr: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    CreateUser {
        #[arg(long)]
        name: String,
        #[arg(long)]
        email: String,
        #[arg(long)]
        password: String,
    },
    AddSshKey {
        #[arg(long)]
        user_id: String,
        #[arg(long)]
        key: String,
        #[arg(long, default_value = "default")]
        title: String,
    },
    CreateToken {
        #[arg(long)]
        user_id: String,
        #[arg(long)]
        read: bool,
        #[arg(long)]
        write: bool,
        #[arg(long)]
        admin: bool,
        #[arg(long)]
        expires_days: Option<i64>,
    },
    SetPermission {
        #[arg(long)]
        repo_id: String,
        #[arg(long)]
        user_id: String,
        #[arg(long)]
        read: bool,
        #[arg(long)]
        write: bool,
        #[arg(long)]
        admin: bool,
    },
    SetOwner {
        #[arg(long)]
        repo_id: String,
        #[arg(long)]
        user_id: String,
    },
}

async fn connect(addr: &str) -> Result<AuthServiceClient<Channel>> {
    let endpoint = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    };

    let channel = Channel::from_shared(endpoint)?
        .connect()
        .await
        .map_err(|e| anyhow::anyhow!("failed to connect to auth service: {}", e))?;

    Ok(AuthServiceClient::new(channel))
}

async fn create_user(
    client: &mut AuthServiceClient<Channel>,
    name: String,
    email: String,
    password: String,
) -> Result<()> {
    let request = CreateUserRequest {
        name,
        email: email.clone(),
        password,
    };

    let response = client.create_user(request).await?.into_inner();

    if response.error.is_empty() {
        println!("User created successfully");
        println!("  User ID: {}", response.user_id);
        println!("  Email: {}", email);
    } else {
        anyhow::bail!("Failed to create user: {}", response.error);
    }

    Ok(())
}

async fn add_ssh_key(
    client: &mut AuthServiceClient<Channel>,
    user_id: String,
    public_key: String,
    title: String,
) -> Result<()> {
    let request = AddSshKeyRequest {
        user_id: user_id.clone(),
        public_key,
        title: title.clone(),
    };

    let response = client.add_ssh_key(request).await?.into_inner();

    if response.error.is_empty() {
        println!("SSH key added successfully");
        println!("  Key ID: {}", response.key_id);
        println!("  Fingerprint: {}", response.fingerprint);
        println!("  Title: {}", title);
        println!("  User ID: {}", user_id);
    } else {
        anyhow::bail!("Failed to add SSH key: {}", response.error);
    }

    Ok(())
}

async fn create_token(
    client: &mut AuthServiceClient<Channel>,
    user_id: String,
    scope_read: bool,
    scope_write: bool,
    scope_admin: bool,
    expires_days: Option<i64>,
) -> Result<()> {
    let expires_at = expires_days.map(|days| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        now + (days * 24 * 60 * 60)
    });

    let request = CreateTokenRequest {
        user_id: user_id.clone(),
        scope_read,
        scope_write,
        scope_admin,
        expires_at: expires_at.unwrap_or(0),
    };

    let response = client.create_token(request).await?.into_inner();

    if response.error.is_empty() {
        println!("Token created successfully");
        println!("  Token ID: {}", response.token_id);
        println!("  Token: {}", response.token);
        println!("  User ID: {}", user_id);
        println!(
            "  Scopes: read={}, write={}, admin={}",
            scope_read, scope_write, scope_admin
        );
        if let Some(days) = expires_days {
            println!("  Expires in: {} days", days);
        } else {
            println!("  Expires: never");
        }
        println!();
        println!("IMPORTANT: Save this token now. It cannot be retrieved again.");
    } else {
        anyhow::bail!("Failed to create token: {}", response.error);
    }

    Ok(())
}

async fn set_permission(
    client: &mut AuthServiceClient<Channel>,
    repo_id: String,
    user_id: String,
    read: bool,
    write: bool,
    admin: bool,
) -> Result<()> {
    let mut permissions: u32 = 0;
    if read {
        permissions |= 0x01;
    }
    if write {
        permissions |= 0x02;
    }
    if admin {
        permissions |= 0x04;
    }

    let request = SetPermissionRequest {
        repo_id: repo_id.clone(),
        user_id: user_id.clone(),
        permissions,
    };

    let response = client.set_permission(request).await?.into_inner();

    if response.success {
        println!("Permission set successfully");
        println!("  Repo: {}", repo_id);
        println!("  User: {}", user_id);
        println!(
            "  Permissions: read={}, write={}, admin={}",
            read, write, admin
        );
    } else {
        anyhow::bail!("Failed to set permission: {}", response.error);
    }

    Ok(())
}

async fn set_owner(
    client: &mut AuthServiceClient<Channel>,
    repo_id: String,
    user_id: String,
) -> Result<()> {
    set_permission(client, repo_id, user_id, true, true, true).await
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let args = Args::parse();

    let mut client = connect(&args.auth_addr).await?;

    match args.command {
        Command::CreateUser {
            name,
            email,
            password,
        } => {
            create_user(&mut client, name, email, password).await?;
        }
        Command::AddSshKey {
            user_id,
            key,
            title,
        } => {
            add_ssh_key(&mut client, user_id, key, title).await?;
        }
        Command::CreateToken {
            user_id,
            read,
            write,
            admin,
            expires_days,
        } => {
            create_token(&mut client, user_id, read, write, admin, expires_days).await?;
        }
        Command::SetPermission {
            repo_id,
            user_id,
            read,
            write,
            admin,
        } => {
            set_permission(&mut client, repo_id, user_id, read, write, admin).await?;
        }
        Command::SetOwner { repo_id, user_id } => {
            set_owner(&mut client, repo_id, user_id).await?;
        }
    }

    Ok(())
}
