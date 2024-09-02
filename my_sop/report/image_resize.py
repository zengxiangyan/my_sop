import cv2
import numpy as np


# 读取图像
image = cv2.imread('cropped_screenshot.png')

# 应用中值滤波去噪
# image_denoised = cv2.medianBlur(image, 5)

# # 应用拉普拉斯算子进行锐化
laplacian_filter = np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]], dtype=np.float32)
image_sharpened = cv2.filter2D(image, -1, laplacian_filter)

# 保存处理后的图像
cv2.imwrite('cropped_screenshot1.png', image_sharpened)
