from scripts.extract import get_videos, get_comments, search_video_ids, get_search_mockup, get_comments_mockup

def main():
    # print(get_comments("iUAnSUgbfBA"))
    # print(get_videos("เลือกตั้ง69"))
    # print(search_video_ids("เลือกตั้ง69"))
    # print(get_search_mockup())
    response = get_search_mockup()
    l = [item['id']['videoId'] for item in response.get('items', [])]
    print(l)



if __name__ == "__main__":
    main()
