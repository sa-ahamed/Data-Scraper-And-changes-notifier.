import os
import difflib
import argparse

def compare_files(old_file, new_file, output_dir):
    """Compare two markdown files and save differences if found."""
    with open(old_file, "r", encoding="utf-8") as f1, open(new_file, "r", encoding="utf-8") as f2:
        old_text = f1.readlines()
        new_text = f2.readlines()

    diff = list(difflib.unified_diff(
        old_text, new_text,
        fromfile=old_file,
        tofile=new_file,
        lineterm=""
    ))

    if diff:
        page_name = os.path.basename(new_file).replace(".md", "")
        changes_file = os.path.join(output_dir, f"{page_name}_changes.md")

        with open(changes_file, "w", encoding="utf-8") as f:
            f.write(f"# Changes in {page_name}\n\n")
            f.write("```diff\n")
            f.write("\n".join(diff))
            f.write("\n```")

        print(f"[+] Changes found → {changes_file}")
    else:
        print(f"[=] No changes → {os.path.basename(new_file)}")

def compare_snapshots(old_dir, new_dir, output_dir="changes"):
    """Compare two directories of markdown snapshots."""
    os.makedirs(output_dir, exist_ok=True)

    old_files = {f: os.path.join(old_dir, f) for f in os.listdir(old_dir) if f.endswith(".md")}
    new_files = {f: os.path.join(new_dir, f) for f in os.listdir(new_dir) if f.endswith(".md")}

    for filename, new_path in new_files.items():
        if filename in old_files:
            compare_files(old_files[filename], new_path, output_dir)
        else:
            print(f"[+] New page detected → {filename}")
            # Save full new file as change record
            page_name = filename.replace(".md", "")
            changes_file = os.path.join(output_dir, f"{page_name}_changes.md")
            with open(new_path, "r", encoding="utf-8") as fsrc, open(changes_file, "w", encoding="utf-8") as fdst:
                fdst.write(f"# New page detected: {page_name}\n\n")
                fdst.write(fsrc.read())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare two markdown snapshots of a website")
    parser.add_argument("old", help="Old snapshot folder")
    parser.add_argument("new", help="New snapshot folder")
    parser.add_argument("--output", default="changes", help="Output folder for changes")
    args = parser.parse_args()

    compare_snapshots(args.old, args.new, args.output)
