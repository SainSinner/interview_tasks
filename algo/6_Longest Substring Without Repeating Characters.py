class Solution:
    def lengthOfLongestSubstring(self, s: str) -> int:

        if len(s) == 0:
            return 0

        answer = []

        for i in range(len(s)):
            s_array = []
            for j in range(i, len(s)):
                if s[j] not in s_array:
                    s_array.append(s[j])
                else:
                    break
            flag = 0
            answer.append("".join(s_array))
            s_array.clear()

        return int(max(map(len, answer)))