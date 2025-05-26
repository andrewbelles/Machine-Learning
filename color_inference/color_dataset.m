% Checks if an image contains the specified hue to mark csv for training
function l = contains_hue(hue_vector, target_hue, delta)
    % Checks for delta passed as argument
    if nargin < 3
        delta = 0.03;
    end

    wrap_diff = abs(angle( ...
        exp(2j*pi*(hue_vector - target_hue))) / (2*pi) ...
    );
    l = any(wrap_diff < delta);
end

% Creates N samples for dataset 
function generate_color_dataset(output_dir, sample_count, delta)
    rng(10);

    % generate csv file of file labels
    csv_file = fullfile(output_dir, 'labels.csv');
    optr     = fopen(csv_file, 'w');
    fprintf(optr, "filename,hue,label\n");
    
 
    spd  = ceil(sample_count/3);       % Samples per difficulty tier 
    dims = [256, 256];                 % 256x256 px 

    % Lambda for writing row of csv 
    write_row = @(name, r_hue, mask) ... 
      fprintf(optr, "%s,%.5f,%d\n", name, r_hue, mask);

    % Easy Tier - Simple Gradient 
    for i = 1:spd
        r_hue = rand;
        hue = linspace(rand, rand, dims(2));
        hsv = cat(3, ...
            repmat(hue, dims(1), 1), ...
            ones(dims), ...
            ones(dims));
        rgb = hsv2rgb(hsv);

        filename = sprintf("easy_%04d.png", i);
        imwrite(rgb, fullfile(output_dir, filename));
        write_row(filename, r_hue, contains_hue(hue, r_hue, delta));
    end

    % Medium Tier - Bilinear Blend 
    for i = 1:spd
        r_hue = rand;
        hue1 = rand; 
        hue2 = rand; 
    
        % Generate grid of colors and make bilinear gradient
        [X, Y] = meshgrid(linspace(0,1,dims(2)), ...
                  linspace(0,1,dims(1)));
        hue = (1 - X) .* hue1 + X .* hue2;
        hue3 = rand;
        hue = (1 - Y) .* hue + Y .*hue3;
        hsv = cat(3, hue, ones(dims), ones(dims));
        rgb = hsv2rgb(hsv);

        filename = sprintf("medium_%04d.png", i);
        imwrite(rgb, fullfile(output_dir, filename));
        write_row(filename, r_hue, contains_hue(hue(:), r_hue, delta));
    end

    % Hard Tier - Radial Noise 
    for i = 1:spd
        r_hue = rand;
        % Generate parameters for radial noise 
        cx = rand * dims(2);
        cy = rand * dims(1);
        [X, Y] = meshgrid(1:dims(2), ...
                  1:dims(1));
        R = sqrt((X - cx).^2 + (Y - cy).^2);
        R = R / max(R(:));

        % Generate noise and scale to 0 to 1 
        noise = imresize(randn(dims/8), dims, 'bicubic');
        noise = (noise - min(noise(:))) / (max(noise(:)) - min(noise(:)));

        hue = mod(R + 0.3 * noise + rand, 1);
        hsv = cat(3, hue, ones(dims), ones(dims));
        rgb = hsv2rgb(hsv);

        filename = sprintf("hard_%04d.png", i);
        imwrite(rgb, fullfile(output_dir, filename));
        write_row(filename, r_hue, contains_hue(hue(:), r_hue, delta));
    end

    fclose(optr);
end

% output_dir, image_count, delta
generate_color_dataset("images", 4096, 0.03);
