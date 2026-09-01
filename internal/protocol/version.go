package protocol

const CurrentVersion uint16 = 1

var supportedVersions = []uint16{CurrentVersion}

func SupportedVersions() []uint16 {
	result := make([]uint16, len(supportedVersions))
	copy(result, supportedVersions)

	return result
}

func IsSupported(version uint16) bool {
	for _, supported := range supportedVersions {
		if supported == version {
			return true
		}
	}

	return false
}
