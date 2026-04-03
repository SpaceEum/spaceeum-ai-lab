import os
import sys

# 1. 대상 확장자 및 제외 폴더 설정
target_extensions = ('.ts', '.js', '.py', '.env', '.csv', '.json', '.txt', '.md')
exclude_dirs = {'node_modules', '.git', 'dist', 'build', '__pycache__'}
output_file = 'TOTAL_PROJECT_CONTEXT.txt'

def merge_files_with_progress():
    # 전체 파일 개수 파악 (진행률 계산용)
    all_files = []
    for root, dirs, files in os.walk('./'):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        for file in files:
            if file.endswith(target_extensions):
                all_files.append(os.path.join(root, file))
    
    total_count = len(all_files)
    if total_count == 0:
        print("❌ 합칠 파일이 없습니다.")
        return

    print(f"🚀 총 {total_count}개의 파일을 합치기 시작합니다...")

    with open(output_file, 'w', encoding='utf-8') as outfile:
        for i, file_path in enumerate(all_files, 1):
            # 진행률 계산 및 표시
            percent = (i / total_count) * 100
            bar = '█' * int(percent / 2) + '-' * (50 - int(percent / 2))
            
            # 터미널 한 줄에서 계속 갱신 (\r 사용)
            sys.stdout.write(f'\r|{bar}| {percent:.1f}% ({i}/{total_count}) - {os.path.basename(file_path)[:20]}...   ')
            sys.stdout.flush()

            # 파일 내용 쓰기
            outfile.write(f"\n\n{'#'*60}\n")
            outfile.write(f"### FILE PATH: {file_path}\n")
            outfile.write(f"{'#'*60}\n\n")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as infile:
                    outfile.write(infile.read())
            except Exception as e:
                outfile.write(f"// [Error reading file: {e}]\n")

    print(f"\n\n✅ 모든 작업 완료! '{output_file}' 파일이 생성되었습니다.")

if __name__ == "__main__":
    merge_files_with_progress()