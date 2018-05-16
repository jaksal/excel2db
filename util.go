package main

func removeDuplicate(in []string) []string {
	duEntry := make(map[string]bool)

	var result []string
	for _, i := range in {
		if _, ok := duEntry[i]; !ok {
			result = append(result, i)
			duEntry[i] = true
		}
	}
	return result
}
