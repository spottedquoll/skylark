function meta = initialise_meta_object(env)
%initialise_meta_object 

    % Global root definitions
    meta.n_sec_root_gl = 6357;
    meta.n_reg_root_gl = 221;

    % Root country legend
    meta.root_country_legend = read_root_country_legend([env.ielab_global_root '/Legends/']);

    % Check the environment
    assert(exist(env.nesting_dir, 'dir'));

    assert(exist(env.tensor_toolbox, 'dir'));
    addpath(genpath(env.tensor_toolbox));

    % Paths
    meta.save_dir = [env.nesting_dir 'objects/kite/'];
    if ~directory_exists(meta.save_dir)
        mkdir(meta.save_dir);
    end

end