use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::process::Command;
use std::sync::Mutex;

use clap::Parser;
use rayon::prelude::*;
use regex::Regex;
use serde_json::Value;
use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Starting directory to check (default: .)
    #[arg(long, default_value = ".")]
    start_path: String,

    /// Package file to read (default: packages.txt)
    #[arg(long, default_value = "packages.txt")]
    package_file: String,

    /// Only check the start directory
    #[arg(long)]
    root_only: bool,

    /// Only list directories to be checked
    #[arg(long)]
    list_dirs: bool,

    /// Number of worker threads to use
    #[arg(short = 'j', long, default_value_t = num_cpus::get())]
    jobs: usize,

    /// Skip calling npm (fast)
    #[arg(long = "no-npm")]
    no_npm: bool,

    /// Verbose logging (debug)
    #[arg(short, long)]
    verbose: bool,
}

struct Preload {
    yarn: Option<String>,
    plock: Option<Value>,
    pnpm: Option<String>,
    deps: Option<String>,
    pkg_json: Option<Value>,
}

fn parse_version(v: &str) -> Option<(i32, i32, i32)> {
    let re = Regex::new(r"^\d+\.\d+\.\d+").unwrap();
    re.captures(v).map(|cap| {
        let parts: Vec<i32> = cap[0]
            .split('.')
            .map(|s| s.parse().unwrap_or(0))
            .collect();
        (parts[0], parts[1], parts[2])
    })
}

fn satisfies_range(version: &str, range: &str) -> bool {
    let version = version.trim_start_matches('^').trim_start_matches('~');
    if let Some((v_major, v_minor, v_patch)) = parse_version(version) {
        if range.starts_with('^') {
            let range_version = range.trim_start_matches('^');
            if let Some((r_major, r_minor, _)) = parse_version(range_version) {
                v_major == r_major && (v_minor > r_minor || (v_minor == r_minor && v_patch >= 0))
            } else {
                false
            }
        } else if range.starts_with('~') {
            let range_version = range.trim_start_matches('~');
            if let Some((r_major, r_minor, r_patch)) = parse_version(range_version) {
                v_major == r_major && v_minor == r_minor && v_patch >= r_patch
            } else {
                false
            }
        } else {
            version == range
        }
    } else {
        false
    }
}

fn find_dirs(root: &Path, root_only: bool) -> Vec<String> {
    let patterns = vec!["package.json"];
    let exclude_dirs = vec![".nx"];
    let mut dirs: HashSet<String> = HashSet::new();

    for entry in WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            !e.path()
                .components()
                .any(|c| exclude_dirs.contains(&c.as_os_str().to_str().unwrap_or("")))
        })
    {
        if entry.file_type().is_file() {
            let file_name = entry.file_name().to_str().unwrap_or("");
            if patterns.contains(&file_name) {
                if let Some(parent) = entry.path().parent() {
                    let dir_str = parent.to_str().unwrap_or(".").to_string();
                    dirs.insert(dir_str);
                }
            }
        }
    }

    if root_only {
        let root_str = root.to_str().unwrap_or(".").to_string();
        let root_path = Path::new(&root_str);
        let has_relevant_file = patterns.iter().any(|p| root_path.join(p).is_file());
        if has_relevant_file {
            dirs.insert(root_str);
        }
    } else {
        let root_str = root.to_str().unwrap_or(".").to_string();
        let root_path = Path::new(&root_str);
        let has_relevant_file = patterns.iter().any(|p| root_path.join(p).is_file());
        if has_relevant_file {
            dirs.insert(root_str);
        }
    }

    let mut sorted_dirs: Vec<String> = dirs.into_iter().collect();
    sorted_dirs.sort();
    sorted_dirs
}

fn get_pkg_range(name: &str, pkg_json: Option<&Value>) -> String {
    if let Some(data) = pkg_json {
        for section in ["dependencies", "devDependencies"] {
            if let Some(deps) = data.get(section).and_then(|d| d.as_object()) {
                if let Some(r) = deps.get(name).and_then(|r| r.as_str()) {
                    return r.to_string();
                }
            }
        }
    }
    String::new()
}

fn get_yarn_versions(name: &str, content: &str) -> HashSet<String> {
    let mut versions: HashSet<String> = HashSet::new();
    let record_re = Regex::new(r"\n\s*\n").unwrap();
    let records: Vec<&str> = record_re.split(content).collect();
    let ver_re = Regex::new(r#"version "(\d+\.\d+\.\d+)"#).unwrap();
    for rec in records {
        if rec.contains(&format!("{}@", name)) {
            if let Some(cap) = ver_re.captures(rec) {
                versions.insert(cap[1].to_string());
            }
        }
    }
    versions
}

fn get_package_lock_versions(name: &str, package_lock_json: &Value) -> HashSet<String> {
    let mut versions: HashSet<String> = HashSet::new();
    if let Some(deps) = package_lock_json.get("dependencies").and_then(|d| d.as_object()) {
        if let Some(v) = deps.get(name).and_then(|v| v.get("version")).and_then(|v| v.as_str()) {
            versions.insert(v.to_string());
        }
    }
    if let Some(packages) = package_lock_json.get("packages").and_then(|p| p.as_object()) {
        let key = format!("node_modules/{}", name);
        if let Some(v) = packages.get(&key).and_then(|v| v.get("version")).and_then(|v| v.as_str()) {
            versions.insert(v.to_string());
        }
    }
    if let Some(deps) = package_lock_json.get("dependencies").and_then(|d| d.as_object()) {
        for (k, v) in deps {
            if k == name {
                if let Some(ver) = v.get("version").and_then(|vv| vv.as_str()) {
                    versions.insert(ver.to_string());
                }
            }
            if let Some(sub_obj) = v.as_object() {
                walk_plock(sub_obj, name, &mut versions);
            }
        }
    }
    versions
}

fn walk_plock(obj: &serde_json::Map<String, Value>, name: &str, versions: &mut HashSet<String>) {
    if let Some(deps) = obj.get("dependencies").and_then(|d| d.as_object()) {
        for (k, v) in deps {
            if k == name {
                if let Some(ver) = v.get("version").and_then(|vv| vv.as_str()) {
                    versions.insert(ver.to_string());
                }
            }
            if let Some(sub_obj) = v.as_object() {
                walk_plock(sub_obj, name, versions);
            }
        }
    }
}

fn get_pnpm_versions(name: &str, content: &str) -> HashSet<String> {
    let mut versions: HashSet<String> = HashSet::new();
    let pattern = Regex::new(&format!(r"/{}/(\d+\.\d+\.\d+)", regex::escape(name))).unwrap();
    for cap in pattern.captures_iter(content) {
        versions.insert(cap[1].to_string());
    }
    let pattern2 = Regex::new(&format!(r#""{}@(\d+\.\d+\.\d+)"#, regex::escape(name))).unwrap();
    for cap in pattern2.captures_iter(content) {
        versions.insert(cap[1].to_string());
    }
    versions
}

fn get_dependencies_versions(name: &str, content: &str) -> HashSet<String> {
    let mut versions: HashSet<String> = HashSet::new();
    let pattern = Regex::new(&format!(r#""name"\s*:\s*"{}@(\d+\.\d+\.\d+)"#, regex::escape(name))).unwrap();
    for cap in pattern.captures_iter(content) {
        versions.insert(cap[1].to_string());
    }
    if let Ok(data) = serde_json::from_str::<Value>(content) {
        walk_deps(&data, name, &mut versions);
    }
    versions
}

fn walk_deps(obj: &Value, name: &str, versions: &mut HashSet<String>) {
    match obj {
        Value::Object(map) => {
            if let Some(nm) = map.get("name").and_then(|n| n.as_str()) {
                if nm.starts_with(&format!("{}@", name)) {
                    let parts: Vec<&str> = nm.split('@').collect();
                    if parts.len() == 2 && Regex::new(r"^\d+\.\d+\.\d+$").unwrap().is_match(parts[1]) {
                        versions.insert(parts[1].to_string());
                    }
                }
            }
            for (_, v) in map {
                walk_deps(v, name, versions);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                walk_deps(item, name, versions);
            }
        }
        _ => {}
    }
}

fn get_npm_versions(dirpath: &str, name: &str) -> HashSet<String> {
    let mut versions: HashSet<String> = HashSet::new();
    let output = match Command::new("npm")
        .args(["ls", "--json", name, "--depth=Infinity"])
        .current_dir(dirpath)
        .output()
    {
        Ok(o) if o.status.success() => o.stdout,
        _ => return versions,
    };
    let output_str = match std::str::from_utf8(&output) {
        Ok(s) => s,
        _ => return versions,
    };
    let data: Value = match serde_json::from_str(output_str) {
        Ok(d) => d,
        _ => return versions,
    };
    walk_npm(&data, name, &mut versions);
    versions
}

fn walk_npm(obj: &Value, name: &str, versions: &mut HashSet<String>) {
    if let Value::Object(map) = obj {
        if let Some(deps) = map.get("dependencies").and_then(|d| d.as_object()) {
            for (k, v) in deps {
                if k == name {
                    if let Some(ver) = v.get("version").and_then(|vv| vv.as_str()) {
                        versions.insert(ver.to_string());
                    }
                }
                walk_npm(v, name, versions);
            }
        }
    }
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    rayon::ThreadPoolBuilder::new()
        .num_threads(args.jobs)
        .build_global()
        .unwrap();

    if args.verbose {
        eprintln!("[debug] Using {} threads", args.jobs);
    }

    println!("Checking for npm packages and lockfile/package.json/DEPENDENCIES.json compatibility in this project and subfolders...");

    let start_path = Path::new(&args.start_path);
    let dirs = find_dirs(start_path, args.root_only);

    eprintln!("Directories to be checked:");
    for d in &dirs {
        eprintln!("  {}", d);
    }

    if args.list_dirs {
        return Ok(());
    }

    if dirs.is_empty() {
        eprintln!("[warning] No directories found with package.json");
        return Ok(());
    }

    // Read package file from start_path
    let packages_file_path = Path::new(&args.package_file);
    let packages_file = match File::open(&packages_file_path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("[error] Failed to open {} at {}: {}", args.package_file, packages_file_path.display(), e);
            return Ok(());
        }
    };
    let packages: HashSet<(String, String)> = BufReader::new(packages_file)
        .lines()
        .filter_map(|line| {
            if let Ok(l) = line {
                let parts: Vec<&str> = l.trim().split('@').collect();
                if parts.len() == 2 {
                    Some((parts[0].to_string(), parts[1].to_string()))
                } else {
                    if args.verbose {
                        eprintln!("[warning] Invalid line in {}: {}", args.package_file, l);
                    }
                    None
                }
            } else {
                None
            }
        })
        .collect();

    if packages.is_empty() {
        eprintln!("[error] No valid packages found in {} at {}", args.package_file, packages_file_path.display());
        return Ok(());
    }

    if args.verbose {
        eprintln!("[debug] Loaded {} packages from {}", packages.len(), args.package_file);
    }

    // Preload lock files and package.json
    let mut preloads: HashMap<String, Preload> = HashMap::new();
    for d in &dirs {
        let mut preload = Preload {
            yarn: None,
            plock: None,
            pnpm: None,
            deps: None,
            pkg_json: None,
        };
        let dir_path = Path::new(d);
        if let Ok(content) = fs::read_to_string(dir_path.join("yarn.lock")) {
            preload.yarn = Some(content);
        }
        let plock_path = dir_path.join("package-lock.json");
        if plock_path.is_file() {
            if let Ok(file) = File::open(&plock_path) {
                if let Ok(value) = serde_json::from_reader(file) {
                    preload.plock = Some(value);
                }
            }
        }
        if let Ok(content) = fs::read_to_string(dir_path.join("pnpm-lock.yaml")) {
            preload.pnpm = Some(content);
        }
        if let Ok(content) = fs::read_to_string(dir_path.join("DEPENDENCIES.json")) {
            preload.deps = Some(content);
        }
        let pj_path = dir_path.join("package.json");
        if pj_path.is_file() {
            if let Ok(file) = File::open(&pj_path) {
                if let Ok(value) = serde_json::from_reader(file) {
                    preload.pkg_json = Some(value);
                }
            }
        }
        preloads.insert(d.clone(), preload);
    }

    if args.verbose {
        eprintln!("[debug] Preloaded lockfiles and package.json for {} directories", preloads.len());
    }

    // Prepare for parallel processing
    let rows_mutex: Mutex<Vec<(String, String, String, bool, bool, String, String)>> = Mutex::new(Vec::new());
    let found_mutex: Mutex<Vec<String>> = Mutex::new(Vec::new());

    dirs.par_iter().for_each(|d| {
        let preload = preloads.get(d).unwrap();
        let pkg_json = preload.pkg_json.as_ref();

        // Process main package from package.json
        if let Some(data) = pkg_json {
            let name = data.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let version = data.get("version").and_then(|v| v.as_str()).unwrap_or("");
            if !name.is_empty() && !version.is_empty() {
                let match_package = packages.iter().any(|(pkg_name, _)| pkg_name == name);
                let match_version = packages.contains(&(name.to_string(), version.to_string()));

                rows_mutex.lock().unwrap().push((
                    name.to_string(),
                    version.to_string(),
                    d.to_string(),
                    match_package,
                    match_version,
                    String::new(),
                    String::new(),
                ));

                if match_package && match_version {
                    found_mutex
                        .lock()
                        .unwrap()
                        .push(format!("{}:{}@{}", d, name, version));
                }

                // Process dependencies
                if let Some(deps) = data.get("dependencies").and_then(|d| d.as_object()) {
                    for (dep_name, dep_version) in deps {
                        let dep_version = dep_version.as_str().unwrap_or("");
                        let dep_version_clean = dep_version.trim_start_matches('^').trim_start_matches('~');
                        let match_package = packages.iter().any(|(pkg_name, _)| pkg_name == dep_name);
                        let match_version = packages.iter().any(|(pkg_name, pkg_version)| {
                            pkg_name == dep_name && satisfies_range(dep_version_clean, pkg_version)
                        });

                        rows_mutex.lock().unwrap().push((
                            dep_name.to_string(),
                            dep_version_clean.to_string(),
                            d.to_string(),
                            match_package,
                            match_version,
                            "yes".to_string(),
                            format!("{}@{}", name, version),
                        ));

                        if match_package && match_version {
                            found_mutex
                                .lock()
                                .unwrap()
                                .push(format!("{}:{}@{}", d, dep_name, dep_version_clean));
                        }
                    }
                }

                // Process devDependencies
                if let Some(deps) = data.get("devDependencies").and_then(|d| d.as_object()) {
                    for (dep_name, dep_version) in deps {
                        let dep_version = dep_version.as_str().unwrap_or("");
                        let dep_version_clean = dep_version.trim_start_matches('^').trim_start_matches('~');
                        let match_package = packages.iter().any(|(pkg_name, _)| pkg_name == dep_name);
                        let match_version = packages.iter().any(|(pkg_name, pkg_version)| {
                            pkg_name == dep_name && satisfies_range(dep_version_clean, pkg_version)
                        });

                        rows_mutex.lock().unwrap().push((
                            dep_name.to_string(),
                            dep_version_clean.to_string(),
                            d.to_string(),
                            match_package,
                            match_version,
                            "dev".to_string(),
                            format!("{}@{}", name, version),
                        ));

                        if match_package && match_version {
                            found_mutex
                                .lock()
                                .unwrap()
                                .push(format!("{}:{}@{}", d, dep_name, dep_version_clean));
                        }
                    }
                }
            }
        }

        // Process lockfiles and npm ls for additional versions
        for (name, version) in &packages {
            let rng = get_pkg_range(name, pkg_json);
            let mut versions_by_file: HashMap<String, HashSet<String>> = HashMap::new();

            if let Some(content) = &preload.yarn {
                let yv = get_yarn_versions(name, content);
                if !yv.is_empty() {
                    versions_by_file.insert("yarn.lock".to_string(), yv);
                }
            }
            if let Some(plock) = &preload.plock {
                let plv = get_package_lock_versions(name, plock);
                if !plv.is_empty() {
                    versions_by_file.insert("package-lock.json".to_string(), plv);
                }
            }
            if let Some(content) = &preload.pnpm {
                let pnv = get_pnpm_versions(name, content);
                if !pnv.is_empty() {
                    versions_by_file.insert("pnpm-lock.yaml".to_string(), pnv);
                }
            }
            if let Some(content) = &preload.deps {
                let dev = get_dependencies_versions(name, content);
                if !dev.is_empty() {
                    versions_by_file.insert("DEPENDENCIES.json".to_string(), dev);
                }
            }

            let mut nv: HashSet<String> = HashSet::new();
            if !args.no_npm {
                nv = get_npm_versions(d, name);
                if !nv.is_empty() {
                    versions_by_file.insert("npm_installed".to_string(), nv.clone());
                }
            }

            let mut all_versions: HashSet<String> = HashSet::new();
            for versions in versions_by_file.values() {
                all_versions.extend(versions.iter().cloned());
            }
            all_versions.extend(nv.iter().cloned());

            let match_package = !rng.is_empty() || !all_versions.is_empty();
            let match_version = all_versions.iter().any(|v| satisfies_range(v, version));

            if !match_package && !match_version {
                continue;
            }

            rows_mutex.lock().unwrap().push((
                name.clone(),
                version.clone(),
                d.to_string(),
                match_package,
                match_version,
                String::new(),
                String::new(),
            ));

            if match_package && match_version {
                found_mutex
                    .lock()
                    .unwrap()
                    .push(format!("{}:{}@{}", d, name, version));
            }
        }
    });

    // Sort and print found
    let mut found = found_mutex.into_inner().unwrap();
    found.sort();
    for item in found {
        println!("{}", item);
    }

    // Write CSV
    let mut csv_writer = csv::Writer::from_path("output.csv")?;
    csv_writer.write_record(&[
        "package",
        "version",
        "location",
        "match_package",
        "match_version",
        "dependency",
        "depended_by",
    ])?;

    let mut rows = rows_mutex.into_inner().unwrap();
    rows.sort_by_key(|r| (r.0.clone(), r.1.clone(), r.2.clone()));
    for (pkg, ver, loc, mp, mv, dep, dep_by) in rows {
        csv_writer.write_record(&[
            pkg,
            ver,
            loc,
            mp.to_string(),
            mv.to_string(),
            dep,
            dep_by,
        ])?;
    }

    println!("Scan complete.");

    Ok(())
}